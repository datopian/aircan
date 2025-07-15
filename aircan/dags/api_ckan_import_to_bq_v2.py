
import logging
import time
import json
import ast
from datetime import date, datetime

# Local imports
from aircan.dependencies.google_cloud.bigquery_handler_v2 import bq_import_csv
from aircan.dependencies.utils import aircan_status_update

# Third-party library imports
from airflow import DAG
from airflow.exceptions import AirflowException

from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


args = {
    'start_date': days_ago(0),
    'params': {
        "resource": {
            "path": "path/to/my.csv", 
            "format": "CSV",
            "ckan_resource_id": "res-id-123",
            "schema": {
                "fields": "['field1', 'field2']"
            } 
        },
        "ckan_config": {
            "api_key": "API_KEY",
            "site_url": "URL",
        },
        "big_query": {
            "bq_project_id": "bigquery_project_id",
            "bq_dataset_id": "bigquery_dataset_id"
        },
        "output_bucket": str(date.today())
    }
}

dag = DAG(
    dag_id='ckan_api_import_to_bq_v2',
    default_args=args,
    schedule_interval=None
)

def task_import_resource_to_bq(**context):
    ckan_api_key = context['params'].get('ckan_config', {}).get('api_key')
    ckan_site_url = context['params'].get('ckan_config', {}).get('site_url')
    logging.info('Invoking import resource to bigquery')
    logging.info("resource: {}".format(context['params'].get('resource', {})))

    gc_file_url = context['params'].get('big_query', {}).get('gcs_uri')
    bq_project_id = context['params'].get('big_query', {}).get('bq_project_id')
    bq_dataset_id = context['params'].get('big_query', {}).get('bq_dataset_id')
    bq_table_name = context['params'].get('big_query', {}).get('bq_table_name')
    logging.info("bq_table_name: {}".format(bq_table_name))
    
    raw_schema = context['params'].get('resource', {}).get('schema')
    eval_schema = json.loads(raw_schema)
    if isinstance(eval_schema, str):
        eval_schema = ast.literal_eval(eval_schema)
    schema = eval_schema.get('fields')
    logging.info("SCHEMA: {}".format(schema))

    # sample bq_table_id: "bigquerytest-271707.nhs_test.dag_test"
    bq_table_id = '%s.%s.%s' % (bq_project_id, bq_dataset_id, bq_table_name)          
    logging.info('Importing %s to BQ %s' % (gc_file_url, bq_table_id))
    ckan_conf = context['params'].get('ckan_config', {})
    ckan_conf['resource_id'] = context['params'].get('resource', {}).get('ckan_resource_id')
    dag_run_id = context['dag_run'].run_id
    res_id = ckan_conf.get('resource_id')
    try:
        bq_import_csv(bq_table_id, gc_file_url, schema, ckan_conf)
        status_dict = {
        'dag_run_id': dag_run_id,
        'resource_id': res_id,
        'state': 'complete',
        'message': 'Data ingestion completed successfully for "{res_id}".'.format(
                    res_id=res_id),
        'clear_logs': True
        }
        aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
    except Exception as e:
        status_dict = {
        'dag_run_id': dag_run_id,
        'resource_id': res_id,
        'state': 'failed',
        'message': str(e),
        'clear_logs': True
        }
        aircan_status_update(ckan_site_url, ckan_api_key, status_dict)
        raise Exception(str(e))

import_resource_to_bq_task = PythonOperator(
    task_id='import_resource_to_bq_v2',
    provide_context=True,
    python_callable=task_import_resource_to_bq,
    dag=dag,
)
