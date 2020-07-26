
import logging
import time
import json
import ast
from datetime import date, datetime

# Local imports
from aircan.dependencies.google_cloud.bigquery_handler import bq_import_csv

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
    dag_id='ckan_api_import_to_bq',
    default_args=args,
    schedule_interval=None
)

def task_import_resource_to_bq(**context):
    logging.info('Invoking import resource to bigquery')
    gc_file_url = context['params'].get('resource', {}).get('path')
    bq_project_id = context['params'].get('big_query', {}).get('bq_project_id')
    bq_dataset_id = context['params'].get('big_query', {}).get('bq_dataset_id')
    bq_table_name = gc_file_url.split("/")[-1].partition('.')[0].replace('-', '_')

    logging.info("SCHEMA")
    table_schema = context['params'].get('resource', {}).get('schema')

    # sample bq_table_id: "bigquerytest-271707.nhs_test.dag_test"
    bq_table_id = '%s.%s.%s' % (bq_project_id, bq_dataset_id, bq_table_name)          
    logging.info('Importing %s to BQ %s' % (gc_file_url, bq_table_id))
    bq_import_csv(bq_table_id, gc_file_url, table_schema)



import_resource_to_bq_task = PythonOperator(
    task_id='import_resource_to_bq',
    provide_context=True,
    python_callable=task_import_resource_to_bq,
    dag=dag,
)