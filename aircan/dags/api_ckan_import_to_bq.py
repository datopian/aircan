
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
        "path": "path/to/my.csv", 
        "format": "CSV",
        "ckan_resource_id": "res-id-123",
        "schema": "['field1', 'field2']", 
        "ckan_api_key": "API_KEY",
        "ckan_site_url": "URL",
        "output_bucket": str(date.today())
    }
}

dag = DAG(
    dag_id='ckan_api_import_to_bq',
    default_args=args,
    schedule_interval=None
)

def task_import_resource_to_bq(gc_file_url, table_schema, project_id, dataset_id, table_name, **kwargs):
    logging.info('Importing resource to bigquery')
    # sample bq_table_id: "bigquerytest-271707.nhs_test.dag_test"
    bq_table_id = '%s.%s.%s' % (project_id, dataset_id, table_name)
    logging.info('Importing %s to BQ %s' % (gc_file_url, bq_table_id))
    bq_import_csv(bq_table_id, gc_file_url)



import_resource_to_bq_task = PythonOperator(
    task_id='import_resource_to_bq',
    provide_context=True,
    python_callable=task_import_resource_to_bq,
    op_kwargs={'gc_file_url': "{{ params.path }}", 'table_schema': "{{ params.schema }}", 'project_id': "{{ params.project_id }}", 'dataset_id': "{{ params.dataset_id }}", 'table_name': "{{ params.table_name }}"},
    dag=dag,
)