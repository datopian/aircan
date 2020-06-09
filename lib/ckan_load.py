import os
import time
import pandas as pd
import logging

from pprint import pprint

from airflow import DAG
from airflow.models import Variable
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonVirtualenvOperator
from airflow.hooks.http_hook import HttpHook
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='ckan_load',
    default_args=args,
    schedule_interval=None,
    tags=['conversion']
)


delete_datastore_table_json={
    "resource_id": "htest",
    'force': True
}

delete_datastore_table_url = 'api/3/action/datastore_delete'

def delete_datastore_table_response(response):
    if (response.status_code == 200 or response.status_code == 404):
        return True
    else:
        logging.error("HTTP error: %s", response.reason)
        logging.error(response.text)
        raise AirflowException(str(response.status_code) + ":" + response.reason)


def task_delete_datastore_table():
    logging.info('Invoking Delete Datastore')
    delete_hook = HttpHook(
        method='DELETE',
        http_conn_id='ckan_default')

    response = delete_hook.run(delete_datastore_table_url, data=delete_datastore_table_json,
        headers={"Authorization": Variable.get("CKAN_SYSADMIN_API_KEY")},
        extra_options={'check_response': False})

    return delete_datastore_table_response(response)


delete_datastore_table_task = PythonOperator(
    task_id="delete_datastore_table",
    provide_context=False,
    python_callable=task_delete_datastore_table,
    dag=dag
)

create_datastore_table_url = 'api/3/action/datastore_create'


def task_create_datastore_table():
    logging.info('Invoking Create Datastore')
    create_hook = HttpHook(
        method='POST',
        http_conn_id='ckan_default')

    data_dict = dict(
        resource_id="htest",
        fields=[
            {
                "id": "Goal",
                "type": "text"
            }])
    data_dict['records'] = None  # just create an empty table
    data_dict['force'] = True


    logging.info(data_dict)

    response = create_hook.run(create_datastore_table_url, data=data_dict,
        headers={"Authorization": Variable.get("CKAN_SYSADMIN_API_KEY")})

    return response.status_code


create_datastore_table_task = PythonOperator(
    task_id="create_datastore_table",
    provide_context=False,
    python_callable=task_create_datastore_table,
    dag=dag
)



delete_datastore_table_task
