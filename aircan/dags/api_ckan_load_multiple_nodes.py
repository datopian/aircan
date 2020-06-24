import logging
import time
import json
import ast
from datetime import datetime

# Local imports
from aircan.dependencies.hybrid_load import delete_datastore_table, create_datastore_table
from aircan.dependencies.api_load import load_resource_via_api

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
        "resource_id": "res-id-123",
        "schema_fields_array": "['field1', 'field2']", 
        "csv_input": "path/to/my.csv", 
        "json_output": "path/to/my.json"
    }
}

dag = DAG(
    dag_id='ckan_api_load_multiple_steps',
    default_args=args,
    schedule_interval=None
)


def task_delete_datastore_table(resource_id, **kwargs):
    logging.info('Invoking Delete Datastore')
    return delete_datastore_table(resource_id, Variable.get('CKAN_SYSADMIN_API_KEY'), Variable.get('CKAN_SITE_URL'))


delete_datastore_table_task = PythonOperator(
    task_id='delete_datastore_via_api',
    provide_context=True,
    python_callable=task_delete_datastore_table,
    op_kwargs={'resource_id': "{{ params.resource_id }}"},
    dag=dag,
)


def task_create_datastore_table(resource_id, schema_fields, **kwargs):
    logging.info('Invoking Create Datastore')
    data_resource_fields = ast.literal_eval(schema_fields)
    create_datastore_table(resource_id, data_resource_fields, Variable.get('CKAN_SYSADMIN_API_KEY'), Variable.get('CKAN_SITE_URL'),)


create_datastore_table_task = PythonOperator(
    task_id='create_datastore_via_api',
    provide_context=True,
    python_callable=task_create_datastore_table,
    op_kwargs={'resource_id': '{{ params.resource_id }}', 'schema_fields': '{{ params.schema_fields_array }}' },
    dag=dag,
)

def task_convert_csv_to_json(csv_input, json_output, **kwargs):
    logging.info("Converting CSV to JSON")
    convert(csv_input, json_output)
    logging.info("JSON File path: " + json_output)


convert_csv_to_json_task = PythonOperator(
    task_id='convert_csv_to_json',
    provide_context=True,
    python_callable=task_convert_csv_to_json,
    op_kwargs={'csv_input': '{{ params.csv_input }}', 'json_output': '{{ params.json_output }}' },
    dag=dag,
)

def task_load_resource_via_api(resource_id, json_output, **kwargs):
    logging.info('Loading resource via API')
    try:
        with open(json_output) as f:
            records = json.load(f)
            return load_resource_via_api(
                resource_id, records, Variable.get('CKAN_SYSADMIN_API_KEY'), Variable.get('CKAN_SITE_URL'))
    except Exception as e:
        # raise AirflowException(str(response.status_code) + ":" + response.reason)
        return {"success": False, "errors": [e]}

load_resource_via_api_task = PythonOperator(
    task_id='load_resource_via_api',
    provide_context=True,
    python_callable=task_load_resource_via_api,
    op_kwargs={'resource_id': '{{ params.resource_id }}', 'json_output': '{{ params.json_output }}' },
    dag=dag,
)


delete_datastore_table_task >> create_datastore_table_task >> convert_csv_to_json_task >> load_resource_via_api_task
