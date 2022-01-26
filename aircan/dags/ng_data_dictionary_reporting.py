import logging
import datetime
import time
from pprint import pprint

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
import requests
from urllib.parse import urljoin
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from string import Template
from time import sleep

CKAN_URL =  Variable.get("NG_CKAN_URL")
SENDGRID_KEY =  Variable.get("NG_SENDGRID_API_KEY")
MAIL_FROM = Variable.get("NG_MAIL_FROM")
MAIL_TO = Variable.get("NG_MAIL_TO")

args = {
    'start_date': days_ago(0)
}

dag = DAG(
    dag_id='ng_data_example_logs',
    default_args=args,
    schedule_interval='0 0 * * *'
)

def get_all_datastore_tables():
    try:
        logging.info("Retriving all datastore tables.")
        response = requests.get(
            urljoin(CKAN_URL, '/api/3/action/datastore_search?resource_id=_table_metadata')
        )
        response.raise_for_status()
        if response.status_code == 200:
           return [resource['name'] for resource in response.json()['result']['records']]
            
    except requests.exceptions.HTTPError as e:
            return e.response.text 


def get_data_dictionary(res_id):
    try:
        logging.info("Retriving fields infomation for {res_id}".format(res_id=res_id))
        response = requests.get(
            urljoin(CKAN_URL, 
                    '/api/3/action/datastore_search?resource_id={0}&limit=0'.format(res_id))
        )
        response.raise_for_status()
        if response.status_code == 200:
            return response.json()['result']['fields']

    except requests.exceptions.HTTPError as e:
        logging.error("Failed to get fields infomation for {res_id}".format(res_id=res_id))
        return False


def check_empty_data_dictionary(ds, **kwargs):
    all_datastore_tables = get_all_datastore_tables()
    all_datastore_tables = all_datastore_tables[:30]
    logging.info("Retriving all datastore tables.")
    report = []
    for res_id in all_datastore_tables:
        # Delay 500ms
        sleep(0.5)
        fields = get_data_dictionary(res_id)
        if(fields):
            data_dictionary = ['info' in f for f in fields]
            print(data_dictionary)
            if True not in data_dictionary:
                report.append(res_id)
    return report


def HTML_report_generate(resource_list): 
    table_row = ""
    for resource in resource_list:
        table_row +=  u"<tr><td>{0}</td><td>{1}</td></tr>".format(resource, "Empty") 
               
    html = """\
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                table {
                font-family: arial, sans-serif;
                border-collapse: collapse;
                width: 100%;
                }
                td, th {
                border: 1px solid #dddddd;
                text-align: left;
                padding: 8px;
                }
                tr:nth-child(even) {
                background-color: #dddddd;
                }
            </style>
        </head>
        <body>
            <h2>List of the resource with empty data dictionaries.</h2>
            <table>
                <tr>
                    <th>Resource ID</th>
                    <th>Data Dictionary</th>
                </tr>
                $table_row
            </table>
        </body>
        </html>
    """
    return Template(html).safe_substitute(table_row=table_row)

def dispatch_email(**context):
    resource_list =  context['task_instance'].xcom_pull(task_ids='check_empty_data_dictionary')
    message = Mail(
        from_email=MAIL_FROM,
        to_emails=tuple(MAIL_TO.split(',')),
        subject='Empty data dictionary report.',
        html_content= HTML_report_generate(resource_list))
    try:
        sg = SendGridAPIClient(SENDGRID_KEY)
        response = sg.send(message)
        logging.info("{0} - Report successfully sent via email.".format(response.status_code))
    except Exception as e:
        logging.info(e.message)


check_empty_data_dictionary_task = PythonOperator(
    task_id='check_empty_data_dictionary',
    provide_context=True,
    python_callable=check_empty_data_dictionary,
    dag=dag,
)

dispatch_email_task = PythonOperator(
    task_id='dispatch_email',
    provide_context=True,
    python_callable=dispatch_email,
    dag=dag,
)

check_empty_data_dictionary_task  >> dispatch_email_task