import logging
import time
import json
import ast
from datetime import date, datetime

# Local imports
from aircan.dependencies.hybrid_load import create_datastore_table
from aircan.dependencies.api_load import load_resource_via_api

from aircan.dags.api_ckan_load_gcp import task_delete_datastore_table
from aircan.dependencies.file_conversion.csv_to_json import convert

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
    dag_id='ckan_api_load_multiple_steps',
    default_args=args,
    schedule_interval=None
)


def local_task_delete_datastore_table(**context):
    logging.info('Invoking Delete Datastore')
    return task_delete_datastore_table(**context)


delete_datastore_table_task = PythonOperator(
    task_id='delete_datastore_via_api',
    provide_context=True,
    python_callable=local_task_delete_datastore_table,
    dag=dag,
)


def local_task_create_datastore_table(**context):
    logging.info('Invoking Create Datastore')
    resource_id = context['params'].get('resource', {}).get('ckan_resource_id')
    ckan_api_key = context['params'].get('ckan_config', {}).get('api_key')
    ckan_site_url = context['params'].get('ckan_config', {}).get('site_url')
    raw_schema = context['params'].get('resource', {}).get('schema')
    eval_schema = json.loads(raw_schema)
    eval_schema = ast.literal_eval(eval_schema)
    schema = eval_schema.get('fields')
    return create_datastore_table(resource_id, schema, ckan_api_key, ckan_site_url)


create_datastore_table_task = PythonOperator(
    task_id='create_datastore_via_api',
    provide_context=True,
    python_callable=local_task_create_datastore_table,
    dag=dag,
)

def task_convert_csv_to_json(**context):
    logging.info("Converting CSV to JSON")
    csv_input = context['params'].get('resource', {}).get('path')
    input_file_name = csv_input.split("/")[-1]
    input_file_name = input_file_name.split(".")[0] 
    json_output = input_file_name + '.json'
    convert(csv_input, json_output)
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
    input_file_name = csv_input.split("/")[-1]
    input_file_name = input_file_name.split(".")[0] 
    json_output = input_file_name + '.json'
    ckan_api_key = context['params'].get('ckan_config', {}).get('api_key')
    ckan_site_url = context['params'].get('ckan_config', {}).get('site_url')
    try:
        with open(json_output) as f:
            records = json.load(f)
            records = json.dumps(records)
            return load_resource_via_api(
                resource_id, records, ckan_api_key, ckan_site_url)
    except Exception as e:
        # raise AirflowException(str(response.status_code) + ":" + response.reason)
        return {"success": False, "errors": [e]}

load_resource_via_api_task = PythonOperator(
    task_id='load_resource_via_api',
    provide_context=True,
    python_callable=task_load_resource_via_api,
    dag=dag,
)


delete_datastore_table_task >> create_datastore_table_task >> convert_csv_to_json_task >> load_resource_via_api_task
