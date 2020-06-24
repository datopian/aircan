# Standard library imports
from pprint import pprint
import logging
import os
import pandas as pd
import time

# Local imports
from aircan.dependencies.hybrid_load import *

# Third-party library imports
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook

from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from sqlalchemy import create_engine


args = {
    'start_date': days_ago(0)
}

dag = DAG(
    dag_id='ckan_load',
    default_args=args,
    schedule_interval=None
)

data_resource = {
    'path': './r2.csv',
    'ckan_resource_id': '00dff48a-6537-4652-a430-20397a6f2ea0',
    'schema': {
        'fields': [
            {'name': 'FID', 'type': 'text'}
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
    return load.delete_datastore_table(data_resource, get_config())


delete_datastore_table_task = PythonOperator(
    task_id='delete_datastore_table',
    provide_context=False,
    python_callable=task_delete_datastore_table,
    dag=dag,
)


def task_create_datastore_table():
    logging.info('Invoking Create Datastore')
    return load.create_datastore_table(data_resource, get_config())


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
        data_resource, get_config(), get_connection()
    )


load_csv_to_postgres_via_copy_task = PythonOperator(
    task_id='load_csv_to_postgres_via_copy',
    provide_context=False,
    python_callable=task_load_csv_to_postgres_via_copy,
    dag=dag,
)

def task_restore_indexes_and_set_datastore_active():
    logging.info('Restore Indexes')
    return load.restore_indexes_and_set_datastore_active(
        data_resource, get_config(), get_connection()
    )


restore_indexes_and_set_datastore_active_task = PythonOperator(
    task_id='restore_indexes_and_set_datastore_active',
    provide_context=False,
    python_callable=task_restore_indexes_and_set_datastore_active,
    dag=dag,
)
