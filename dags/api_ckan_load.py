# Standard library imports
from pprint import pprint
import logging
import os
import time
import json

# Local imports
from lib.load import delete_datastore_table, create_datastore_table, load_csv_via_api

from lib.file_conversion.csv_to_json import convert

import examples.csv_to_json

# Third-party library imports
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from sqlalchemy import create_engine


args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='ckan_api_load',
    default_args=args,
    schedule_interval=None,
    tags=['conversion'],
)

data_resource = {
    'path': './r2.csv',
    'ckan_resource_id': '0d1505b5-a8ef-4c3b-b19b-101b8e594d6e',
    'schema': {
        'fields': [
            {'name': 'FID', 'type': 'text'},
            {'name': 'Mkt-RF', 'type': 'text'},
            {'name': 'SMB', 'type': 'text'},
            {'name': 'HML', 'type': 'text'},
            {'name': 'RF', 'type': 'text'}
        ]
    },
}


def get_config():
    config = {}
    config['CKAN_SYSADMIN_API_KEY'] = Variable.get('CKAN_SYSADMIN_API_KEY')
    config['CKAN_SITE_URL'] = Variable.get('CKAN_SITE_URL')
    return config



def task_delete_datastore_table():
    logging.info('Invoking Delete Datastore')
    return delete_datastore_table(data_resource, get_config())


delete_datastore_table_task = PythonOperator(
    task_id='delete_datastore_via_api',
    provide_context=False,
    python_callable=task_delete_datastore_table,
    dag=dag,
)


def task_create_datastore_table(**kwargs):
    logging.info('Invoking Create Datastore')
    return create_datastore_table(data_resource, get_config())


create_datastore_table_task = PythonOperator(
    task_id='create_datastore_via_api',
    provide_context=False,
    python_callable=task_create_datastore_table,
    dag=dag,
)



def task_load_csv_via_api(**kwargs):
    logging.info('Loading CSV via API')
    try:
        with open('/Users/hannelita/Development/freelance/Datopian/aircan/dags/r3.json') as f:
            records = json.load(f)
            return load_csv_via_api(
                "0d1505b5-a8ef-4c3b-b19b-101b8e594d6e", records, get_config())
    except Exception as e:
        # raise AirflowException(str(response.status_code) + ":" + response.reason)
        return {"success": False, "errors": [e]}


load_csv_to_postgres_via_copy_task = PythonOperator(
    task_id='load_csv_via_api',
    provide_context=True,
    python_callable=task_load_csv_via_api,
    dag=dag,
)


delete_datastore_table_task >> create_datastore_table_task >> load_csv_to_postgres_via_copy_task


single_dag_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
    'params': {
        "csv_input": "path/to/my.csv",
        "json_output": "path/to/my.json"
    }
}

single_dag = DAG(
    dag_id='ckan_api_load_single_step',
    default_args=single_dag_args,
    schedule_interval=None,
    tags=['api_load']
)

def full_load(csv_input, json_output, **kwargs):
    logging.info('Invoking Create Datastore')
    resource = create_datastore_table(data_resource, get_config())
    logging.info('Converting resources to json')
    convert(csv_input, json_output)
    logging.info('Loading CSV via API')
    try:
        with open(json_output) as f:
            records = json.load(f)
            return load_csv_via_api(
                resource['response_resource_id'], records, get_config())
    except Exception as e:
        return {"success": False, "errors": [e]}



full_load_task = PythonOperator(
    task_id='full_load_via_api',
    provide_context=True,
    python_callable=full_load,
    op_kwargs={'csv_input': "{{ params.csv_input }}", 'json_output': "{{ params.json_output }}" },
    dag=single_dag
)