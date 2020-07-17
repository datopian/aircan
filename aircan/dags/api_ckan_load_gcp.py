import logging
import time
import json
import ast
from datetime import date, datetime


# Local imports
from aircan.dependencies.hybrid_load import delete_datastore_table, create_datastore_table
from aircan.dependencies.api_load import load_resource_via_api

from aircan.dependencies.file_conversion.csv_to_json import convert_from_url
from aircan.dependencies.google_cloud.file_handler import create_file, get_blob

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
    dag_id='ckan_api_load_gcp',
    default_args=args,
    schedule_interval=None
)

def task_delete_datastore_table(resource_id, ckan_api_key, ckan_site_url, **kwargs):
    logging.info('Invoking Delete Datastore')
    return delete_datastore_table(resource_id, ckan_api_key, ckan_site_url)


delete_datastore_table_task = PythonOperator(
    task_id='delete_datastore_via_api',
    provide_context=True,
    python_callable=task_delete_datastore_table,
    op_kwargs={'resource_id': "{{ params.ckan_resource_id }}", 'ckan_api_key': "{{params.ckan_api_key}}", 'ckan_site_url': "{{params.ckan_site_url}}"},
    dag=dag,
)

def task_create_datastore_table(resource_id, schema_fields, ckan_api_key, ckan_site_url, **kwargs):
    logging.info('Invoking Create Datastore')
    data_resource_fields = ast.literal_eval(schema_fields)
    create_datastore_table(resource_id, data_resource_fields, ckan_api_key, ckan_site_url)


create_datastore_table_task = PythonOperator(
    task_id='create_datastore_via_api',
    provide_context=True,
    python_callable=task_create_datastore_table,
    op_kwargs={'resource_id': "{{ params.ckan_resource_id }}", 'schema_fields': "{{ params.schema }}", 'ckan_api_key': "{{params.ckan_api_key}}", 'ckan_site_url': "{{params.ckan_site_url}}"},
    dag=dag,
)

def task_create_bucket_and_remote_file(csv_input, json_output_bucket, **kwargs):
    input_file_name = csv_input.split("/")[-1]
    input_file_name = input_file_name.split(".")[0] 
    json_output = input_file_name + '.json'
    create_file(json_output_bucket, json_output)

create_bucket_and_remote_file_task = PythonOperator(
    task_id='create_bucket_and_remote_file',
    provide_context=True,
    python_callable=task_create_bucket_and_remote_file,
    op_kwargs={'csv_input': "{{ params.path }}", 'json_output_bucket': "{{ params.output_bucket }}"},
    dag=dag,
)


def task_convert_csv_to_json(csv_input, json_output_bucket, ckan_api_key, **kwargs):
    logging.info("Converting CSV to JSON")
    input_file_name = csv_input.split("/")[-1]
    input_file_name = input_file_name.split(".")[0] 
    json_output = input_file_name + '.json'
    blob_json = get_blob(json_output_bucket, json_output)
    convert_from_url(csv_input, blob_json, ckan_api_key)
    logging.info("JSON File path: " + json_output)


convert_csv_to_json_task = PythonOperator(
    task_id='convert_csv_to_json',
    provide_context=True,
    python_callable=task_convert_csv_to_json,
    op_kwargs={'csv_input': "{{ params.path }}", 'json_output_bucket': "{{ params.output_bucket }}", 'ckan_api_key': "{{params.ckan_api_key}}"},
    dag=dag,
)

def task_load_resource_via_api(resource_id, csv_input, json_output_bucket, ckan_api_key, ckan_site_url, **kwargs):
    logging.info('Loading resource via API')
    try:
        input_file_name = csv_input.split("/")[-1]
        input_file_name = input_file_name.split(".")[0] 
        json_output = input_file_name + '.json'
        blob = get_blob(json_output_bucket, json_output)
        return load_resource_via_api(
            resource_id, blob, ckan_api_key, ckan_site_url)
    except Exception as e:
        # raise AirflowException(str(response.status_code) + ":" + response.reason)
        return {"success": False, "errors": [e]}

load_resource_via_api_task = PythonOperator(
    task_id='load_resource_via_api',
    provide_context=True,
    python_callable=task_load_resource_via_api,
    op_kwargs={'resource_id': "{{ params.resource_id }}", 'csv_input': "{{ params.path }}", 'json_output_bucket': "{{ params.output_bucket }}", 'ckan_api_key': "{{params.ckan_api_key}}", 'ckan_site_url': "{{params.ckan_site_url}}" },
    dag=dag,
)


delete_datastore_table_task >> create_datastore_table_task >> create_bucket_and_remote_file_task >> convert_csv_to_json_task >> load_resource_via_api_task
