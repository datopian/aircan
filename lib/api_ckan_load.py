# Standard library imports
from pprint import pprint
import logging
import os
import pandas as pd
import time

# Local imports
import load

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
    dag_id='ckan_load',
    default_args=args,
    schedule_interval=None,
    tags=['conversion'],
)


data_resource = {
    'path': './r2.csv',
    'ckan_resource_id': '5d1af90f-cc57-4271-a990-1116dd1a400e',
    'schema': {
        'fields': [
            {'name': 'first_name', 'type': 'string'},
            {'name': 'last_name', 'type': 'string'},
            {'name': 'email', 'type': 'string'},
            {'name': 'gender', 'type': 'string'},
            {'name': 'ip_address', 'type': 'string'},
            {'name': 'date', 'type': 'string'},
        ]
    },
}


def task_delete_datastore_table():
    logging.info('Invoking Delete Datastore')
    return load.delete_datastore_table(data_resource)


delete_datastore_table_task = PythonOperator(
    task_id='delete_datastore_table',
    provide_context=False,
    python_callable=task_delete_datastore_table,
    dag=dag,
)


def task_create_datastore_table():
    logging.info('Invoking Create Datastore')
    return load.create_datastore_table(data_resource)


create_datastore_table_task = PythonOperator(
    task_id='create_datastore_table',
    provide_context=False,
    python_callable=task_create_datastore_table,
    dag=dag,
)


def get_connection():
    connection = BaseHook.get_connection('ckan_postgres')
    conn_uri = connection.get_uri()
    logging.info(conn_uri)
    engine = create_engine(conn_uri)
    return engine.raw_connection()


def task_load_csv_to_postgres_via_copy():
    logging.info('Loading CSV to postgres')
    return load.load_csv_to_postgres_via_copy(
        data_resource, {}, get_connection()
    )


load_csv_to_postgres_via_copy_task = PythonOperator(
    task_id='load_csv_to_postgres_via_copy',
    provide_context=False,
    python_callable=task_load_csv_to_postgres_via_copy,
    dag=dag,
)
