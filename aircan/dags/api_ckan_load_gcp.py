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
        "resource": {
            "path": "path/to/my.csv", 
            "format": "CSV",
            "ckan_resource_id": "res-id-123",
            "schema": {
                "fields": [
                    {
                        "name": "Field_Name",
                        "type": "number",
                        "format": "default"
                    }
                ]
            } 
        },
        "ckan_config": {
            "api_key": "API_KEY",
            "site_url": "URL",
        },
        "output_bucket": str(date.today())
    }
}

dag = DAG(
    dag_id='ckan_api_load_gcp',
    default_args=args,
    schedule_interval=None
)

def task_delete_datastore_table(**context):
    logging.info('Invoking Delete Datastore')
    resource_id = context['params'].get('resource', {}).get('ckan_resource_id')
    ckan_api_key = context['params'].get('ckan_config', {}).get('api_key')
    ckan_site_url = context['params'].get('ckan_config', {}).get('site_url')
    return delete_datastore_table(resource_id, ckan_api_key, ckan_site_url)


delete_datastore_table_task = PythonOperator(
    task_id='delete_datastore_via_api',
    provide_context=True,
    python_callable=task_delete_datastore_table,
    dag=dag,
)

def task_create_datastore_table(**context):
    logging.info('Invoking Create Datastore')
    resource_id = context['params'].get('resource', {}).get('ckan_resource_id')
    ckan_api_key = context['params'].get('ckan_config', {}).get('api_key')
    ckan_site_url = context['params'].get('ckan_config', {}).get('site_url')
    raw_schema = context['params'].get('resource', {}).get('schema')
    eval_schema = ast.literal_eval(raw_schema)
    schema = eval_schema.get('fields')
    create_datastore_table(resource_id, schema, ckan_api_key, ckan_site_url)


create_datastore_table_task = PythonOperator(
    task_id='create_datastore_via_api',
    provide_context=True,
    python_callable=task_create_datastore_table,
    dag=dag,
)

def task_create_bucket_and_remote_file(**context):
    csv_input = context['params'].get('resource', {}).get('path')
    json_output_bucket = context['params'].get('output_bucket')
    input_file_name = csv_input.split("/")[-1]
    input_file_name = input_file_name.split(".")[0] 
    json_output = input_file_name + '.json'
    create_file(json_output_bucket, json_output)

create_bucket_and_remote_file_task = PythonOperator(
    task_id='create_bucket_and_remote_file',
    provide_context=True,
    python_callable=task_create_bucket_and_remote_file,
    dag=dag,
)


def task_convert_csv_to_json(**context):
    logging.info("Converting CSV to JSON")
    csv_input = context['params'].get('resource', {}).get('path')
    json_output_bucket = context['params'].get('output_bucket')
    ckan_api_key = context['params'].get('ckan_config', {}).get('api_key')
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
    dag=dag,
)

def task_load_resource_via_api(**context):
    logging.info('Loading resource via API')
    resource_id = context['params'].get('resource', {}).get('ckan_resource_id')
    csv_input = context['params'].get('resource', {}).get('path')
    json_output_bucket = context['params'].get('output_bucket')
    ckan_api_key = context['params'].get('ckan_config', {}).get('api_key')
    ckan_site_url = context['params'].get('ckan_config', {}).get('site_url')
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
    dag=dag,
)


delete_datastore_table_task >> create_datastore_table_task >> create_bucket_and_remote_file_task >> convert_csv_to_json_task >> load_resource_via_api_task
