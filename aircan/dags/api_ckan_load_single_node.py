# Standard library imports
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



single_dag_args = {
    'start_date': days_ago(0)
}

single_dag = DAG(
    dag_id='ckan_api_load_single_step',
    default_args=single_dag_args,
    schedule_interval=None
)


def full_load(resource_id, schema_fields, csv_input, json_output, **kwargs):
    logging.info('Deleting Datastore if exists')
    delete_datastore_table(resource_id, Variable.get('CKAN_SYSADMIN_API_KEY'), Variable.get('CKAN_SITE_URL'))
    logging.info('Invoking Create Datastore')
    data_resource_fields = ast.literal_eval(schema_fields)
    create_datastore_table(resource_id, data_resource_fields, Variable.get('CKAN_SYSADMIN_API_KEY'), Variable.get('CKAN_SITE_URL'))
    logging.info('Converting resources to json')
    convert(csv_input, json_output)
    logging.info('Loading CSV via API')
    try:
        with open(json_output) as f:
            records = json.load(f)
            return load_resource_via_api(
                resource_id, records, Variable.get('CKAN_SYSADMIN_API_KEY'), Variable.get('CKAN_SITE_URL'))
    except Exception as e:
        return {"success": False, "errors": [e]}


full_load_task = PythonOperator(
    task_id='full_load_via_api',
    provide_context=True,
    python_callable=full_load,
    op_kwargs={'resource_id': '{{ params.resource_id }}', 'schema_fields': '{{ params.schema_fields_array }}', 'csv_input': '{{ params.csv_input }}', 'json_output': '{{ params.json_output }}' },
    dag=single_dag
)