"""
### CKAN Datastore Dataloader Dag documentation
This dag reads the CKAN resource files and push data into the CKAN datastore
via datastore API.
"""

import logging
import json
import ast
from textwrap import dedent
from datetime import date, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException

from aircan.dependencies.postgres_loader import (
    load_csv_to_postgres_via_copy,
    delete_index,
    restore_indexes_and_set_datastore_active
    )

from aircan.dependencies.utils import (
    days_ago, get_connection, 
    to_bool, ckan_datstore_loader_failure)
    
from aircan.dependencies.api_loader import (
    fetch_and_read,
    compare_schema,
    create_datastore_table,
    delete_datastore_table,
    load_resource_via_api,
    generate_file_and_load_to_GCP
    )

args = {
    'start_date': days_ago(0),
    'params': { 
        'resource': {
            'path': 'path/to/my.csv', 
            'format': 'CSV',
            'ckan_resource_id': 'res-id-123',
            'schema': {
                'fields': [
                    {
                        'name': 'Field_Name',
                        'type': 'number',
                        'format': 'default'
                    }
                ]
            } 
        },
        'ckan_config': {
            'api_key': 'api_key',
            'site_url': "URL",
            "ckan_datastore_postgres_url": "postgres://user:password@host:port/dbname",
            "aircan_notification_subject": "Aircan Notification",
            "aircan_notification_to": "editor",
            "aircan_load_with_postgres_copy": False,
            "aircan_datastore_chunk_insert_rows_size": 250,
            "aircan_append_or_update_datastore": False,
        },
        'output_bucket': str(date.today())
    },
    'on_failure_callback': ckan_datstore_loader_failure,
}

dag = DAG(
    dag_id='ckan_datastore_loader',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20),
    description='CKAN Datastore data loader',
    tags=['CKAN Datastore loader'],
    doc_md=__doc__,
)


# [START fetch_and_read_data_task]
def task_fetch_and_read(**context):
    logging.info('Fetching resource data from url')
    resource_dict = context['params'].get('resource', {})
    ckan_api_key = context['params'].get('ckan_config', {}).get('api_key')
    ckan_site_url = context['params'].get('ckan_config', {}).get('site_url')
    return fetch_and_read(resource_dict, ckan_site_url, ckan_api_key)


fetch_and_read_data_task = PythonOperator(
    task_id='fetch_resource_data',
    provide_context=True,
    python_callable=task_fetch_and_read,
    dag=dag,
    doc_md=dedent(
        """\
        #### Fetch and read task
        This task detects the source type, metadata and schema using frictionless 
        describe python function and save it to the xcom so that it can be processed 
        by next tasks.
        """
    ),
)
# [END fetch_and_read_data_task]


# [START check_schema_task]
def task_check_schema(**context):
    ti = context['ti']
    resource_dict = context['params'].get('resource', {})
    ckan_api_key = context['params'].get('ckan_config', {}).get('api_key')
    ckan_site_url = context['params'].get('ckan_config', {}).get('site_url')
    raw_schema = context['params'].get('resource', {}).get('schema', False)
    global_append_datastore = context['params'].get('ckan_config', {}).get('aircan_append_or_update_datastore')
    resource_dict['datastore_append_enabled'] = context['params'].get('resource', {}) \
                    .get('datastore_append_or_update', global_append_datastore )
                                   
    if raw_schema and raw_schema != '{}':
        eval_schema = json.loads(raw_schema)
        eval_schema = ast.literal_eval(eval_schema)
        schema = eval_schema.get('fields')
    else:
        xcom_result = ti.xcom_pull(task_ids='fetch_resource_data')
        schema = xcom_result['resource'].get('schema', {}).get('fields', [])

    # compare and get the ckan structure schema
    [create_new_datastore_table, ckan_schema] = compare_schema(
            ckan_site_url, ckan_api_key, resource_dict, schema
        )

    #  Store CKAN structured schema in XCOM
    ti.xcom_push(key='ckan_schema', value=ckan_schema)

    if create_new_datastore_table:
        return ['create_datastore_table', 'push_data_into_datastore']
    elif resource_dict['datastore_append_enabled']:
        return 'push_data_into_datastore'
    else:
        return ['create_datastore_table', 'push_data_into_datastore']
        

check_schema_task = BranchPythonOperator(
    task_id="check_schema",
    python_callable=task_check_schema,
    dag=dag,
    doc_md=dedent(
        """\
        #### Check and compare schema task
        This task fetches the datastore schema if the datastore already exists.
        if new schema has changed compare to old then it just run another task 
        to append data in same datastore table. Otherwise it pass task to delete
        and to create new table.
        """
    ),
)
# [END check_schema_task]


# [START create_datastore_table_task]
def task_create_datastore_table(**context):
    ti = context['ti']
    logging.info('Invoking Create Datastore')
    resource_id = context['params'].get('resource', {}).get('ckan_resource_id')
    ckan_api_key = context['params'].get('ckan_config', {}).get('api_key')
    ckan_site_url = context['params'].get('ckan_config', {}).get('site_url')
    schema = ti.xcom_pull(task_ids='check_schema', key='ckan_schema')
    logging.info('Invoking Delete Datastore')
    delete_datastore_table(resource_id, ckan_api_key, ckan_site_url)
    create_datastore_table(resource_id, schema, ckan_api_key, ckan_site_url)


create_datastore_table_task = PythonOperator(
    task_id='create_datastore_table',
    provide_context=True,
    python_callable=task_create_datastore_table,
    dag=dag,
    doc_md=dedent(
        """\
        #### create new datastore table
        This task deletes the existing ckan datastore table and create new table based 
        on detected schema in 'fetch_and_read_data_task'.
        """
    ),
)
# [END create_datastore_table_task]


# [START push_data_into_datastore_task]
def task_push_data_into_datastore(**context):
    logging.info('Loading resource via API')
    resource_dict = context['params'].get('resource', {})
    ckan_api_key = context['params'].get('ckan_config', {}).get('api_key')
    ckan_site_url = context['params'].get('ckan_config', {}).get('site_url')
    datastore_postgres_url = context['params'].get('ckan_config', {}).get('ckan_datastore_postgres_url')
    load_with_postgres_copy = context['params'].get('ckan_config', {}).get('aircan_load_with_postgres_copy') 
    chunk_size = context['params'].get('ckan_config', {}).get('aircan_datastore_chunk_insert_rows_size') 
    global_append_datastore = context['params'].get('ckan_config', {}).get('aircan_append_or_update_datastore')
    resource_dict['datastore_append_enabled'] = context['params'].get('resource', {}) \
                    .get('datastore_append_or_update', global_append_datastore )

    # Temporary resorce file path from xcom result
    ti = context['ti']
    xcom_result = ti.xcom_pull(task_ids='fetch_resource_data')
    resource_dict['resource_tmp_file'] = xcom_result['resource_tmp_file']

    if to_bool(load_with_postgres_copy):
        raw_schema = context['params'].get('resource', {}).get('schema', False)
        if raw_schema and raw_schema != '{}':
            eval_schema = json.loads(raw_schema)
            schema = ast.literal_eval(eval_schema)
        else:
            schema = xcom_result['resource'].get('schema', {})
        kwargs = {
            'site_url': ckan_site_url, 
            'resource_dict': resource_dict,
            'api_key': ckan_api_key,
            'schema': ti.xcom_pull(task_ids='check_schema', key='ckan_schema'),
        }
        delete_index(resource_dict, connection=get_connection(datastore_postgres_url))
        load_csv_to_postgres_via_copy(connection=get_connection(datastore_postgres_url), **kwargs)
        restore_indexes_and_set_datastore_active(resource_dict, schema, connection=get_connection(datastore_postgres_url))
    else:
        return load_resource_via_api(resource_dict, ckan_api_key, ckan_site_url, chunk_size)


push_data_into_datastore_task = PythonOperator(
    task_id='push_data_into_datastore',
    provide_context=True,
    python_callable=task_push_data_into_datastore,
    trigger_rule='none_failed_or_skipped',
    dag=dag,
    doc_md=dedent(
        """\
        #### create new datastore table
        This task pushes the data into datastore on newly created or exisiting 
        datastore table. 
        """
    ),
)
# [END push_data_into_datastore_task]

# [START generate_file_and_load_to_GCP]
def task_generate_file_and_load_to_GCP(**context):
    resource_dict = context['params'].get('resource', {})
    ckan_config = context['params'].get('ckan_config', {})
    global_append_datastore = context['params'].get('ckan_config', {}).get('aircan_append_or_update_datastore')
    resource_dict['datastore_append_enabled'] = context['params'].get('resource', {}) \
                    .get('datastore_append_or_update', global_append_datastore )
    if not resource_dict['datastore_append_enabled']:
        raise AirflowSkipException('Skipping GCP load as resource not configured to append to datastore')
    generate_file_and_load_to_GCP(resource_dict, ckan_config)
    return  {'success': True}

    
generate_file_and_push_to_GCP = PythonOperator(
    task_id='generate_file_and_load_to_GCP',
    provide_context=True,
    python_callable=task_generate_file_and_load_to_GCP,
    trigger_rule='none_failed_or_skipped',
    dag=dag,
    doc_md=dedent(
        """\
        ####  Generate file and push to GCP
        This task generates file and push to GCP bucket for append enabled resources so that
        it less impact when downloading resource file
        """
    ),
)
# [END generate_file_and_load_to_GCP]

# [SET WORKFLOW ]
check_schema_task.set_upstream(fetch_and_read_data_task)
create_datastore_table_task.set_upstream(check_schema_task)
push_data_into_datastore_task.set_upstream([create_datastore_table_task, check_schema_task])
generate_file_and_push_to_GCP.set_upstream(push_data_into_datastore_task)
# [END WORKFLOW]
