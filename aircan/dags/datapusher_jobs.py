"""
This DAG is developed to check CKAN datapusher stuck jobs and resubmit them automatically at scheduled time.
You need to provide below environment variables. 
    DATAPUSHER_SITE_URL = '[URL of CKAN]'
    DATAPUSHER_API_KEY = '[User API key from CKAN]'
NOTE: This DAG requires custom API '/api/3/action/datapusher_jobs_list' on CKAN which should return list of datapusher 
jobs, otherwise it wouldn't work.
"""

from urllib.parse import urljoin

from datetime import datetime, timedelta
import logging
from airflow.utils.dates import days_ago
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

CKAN_SITE_URL =  Variable.get("DATAPUSHER_SITE_URL")
CKAN_API_KEY = Variable.get("DATAPUSHER_API_KEY")


args = {
    'start_date': days_ago(0)
}

dag = DAG(
    dag_id='datapusher_jobs_checks',
    default_args=args,
    # Everyday at midnight
    schedule_interval='0 0 * * *'
)

def get_datapusher_jobs(ckan_site_url):
    try:
        response = requests.get(
            urljoin(ckan_site_url, '/api/3/action/datapusher_jobs_list')
        )
        response.raise_for_status()
        if response.status_code == 200:
            logging.info(response.json()['result'])
            return response.json()['result']
    except requests.exceptions.HTTPError as e:
            return e.response.text 


def resubmit_datapusher(ckan_site_url, ckan_api_key, resource_id):
    header = {'Authorization': ckan_api_key}
    try:
        response = requests.post(
            urljoin(ckan_site_url, '/api/3/action/datapusher_submit'),
            headers=header,
            json={'resource_id': resource_id}

        )
        response.raise_for_status()
        if response.status_code == 200:
            logging.info('Datapusher successfully resubmitted for {0}'.format(resource_id))
    except requests.exceptions.HTTPError as e:
        return e.response.text 
        

def datapusher_jobs_checks(): 
    logging.info('Retrieving datapusher jobs lists')
    jobs_dict = get_datapusher_jobs(CKAN_SITE_URL)
    for job in jobs_dict:
        if job['state'] == 'pending' and  job['last_updated']:
            time_since_last_updated = datetime.utcnow() - datetime.fromisoformat('2021-09-15 01:42:36.605660')
             # Only submit if it has been pending for the last 5 hours.
            if time_since_last_updated > timedelta(hours=5):
                logging.info('Resubmitting {0}, was pending from {1} ago.'.format(job['entity_id'], time_since_last_updated))
                resubmit_datapusher(CKAN_SITE_URL, CKAN_API_KEY, job['entity_id'])

datapusher_jobs_checks_task = PythonOperator(
    task_id='datapusher_jobs_check',
    provide_context=False,
    python_callable=datapusher_jobs_checks,
    dag=dag,
)
