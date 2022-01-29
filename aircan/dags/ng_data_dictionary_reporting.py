"""
This DAG is developed to generate reports of empty data dictionaries in CKAN Datastore at scheduled times.
You need to provide below environment variables. 
    DATASTORE_REPORT_CKAN_URL = '[URL of CKAN]'
    DATASTORE_REPORT_SENDGRID_API_KEY = '[Sendgrid API]'
    DATASTORE_REPORT_MAIL_FROM = '[Sender email address in notificaiton]'
    DATASTORE_REPORT_MAIL_TO = '[Notificaiton recipients email address]'
    DATASTORE_REPORT_EMAIL_LOGO = '[Provide a logo for email template]'
"""

import logging
import datetime
import time
from pprint import pprint

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
import requests
from urllib.parse import urljoin
from airflow.utils import timezone
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, To
from string import Template
from time import sleep

CKAN_URL =  Variable.get("DATASTORE_REPORT_CKAN_URL")
SENDGRID_KEY =  Variable.get("DATASTORE_REPORT_SENDGRID_API_KEY")
MAIL_FROM = Variable.get("DATASTORE_REPORT_MAIL_FROM")
MAIL_TO = Variable.get("DATASTORE_REPORT_MAIL_TO")
LOGO = Variable.get("DATASTORE_REPORT_EMAIL_LOGO")


args = {
    "start_date": days_ago(0)
}

dag = DAG(
    dag_id="datastore_dictionary_report",
    default_args=args,
    schedule_interval="0 0 * * 0"
)

def get_all_datastore_tables():
    try:
        logging.info("Retriving all datastore tables.")
        response = requests.get(urljoin(CKAN_URL, "/api/3/action/datastore_search?resource_id=_table_metadata&limit=100000"),
                     headers = {"User-Agent": "ckan-others/latest  (internal API call from airflow dag)"})
        response.raise_for_status()
        if response.status_code == 200:
           return [resource["name"] for resource in response.json()["result"]["records"]]
            
    except requests.exceptions.HTTPError as e:
            return e.response.text 


def get_data_dictionary(res_id):
    try:
        logging.info("Retriving fields infomation for {res_id}".format(res_id=res_id))
        response = requests.get(
            urljoin(CKAN_URL, "/api/3/action/datastore_search?resource_id={res_id}&limit=0".format(res_id=res_id)),
                    headers={"User-Agent": "ckan-others/latest (internal API call from airflow dag)"}
        )
        response.raise_for_status()
        if response.status_code == 200:
            return response.json()["result"]["fields"]

    except requests.exceptions.HTTPError as e:
        logging.error("Failed to get fields infomation for {res_id}".format(res_id=res_id))
        return False

def get_resource_detail(res_id):
    try:
        logging.info("Retriving resource infomation for {res_id}".format(res_id=res_id))
        response = requests.get(
            urljoin(CKAN_URL, "/api/3/action/resource_show?id={res_id}".format(res_id=res_id)),
                    headers={"User-Agent": "ckan-others/latest  (internal API call from airflow dag)"}
        )
        response.raise_for_status()
        if response.status_code == 200:
            return response.json()["result"]

    except requests.exceptions.HTTPError as e:
        logging.error("Failed to get resource infomation for {res_id}".format(res_id=res_id))
        return False


def check_empty_data_dictionary(ds, **kwargs):
    all_datastore_tables = get_all_datastore_tables()
    report = []
    for res_id in all_datastore_tables:
        # Delay 500ms
        sleep(0.5)
        fields = get_data_dictionary(res_id)
        if(fields):
            data_dictionary = ['info' in f for f in fields]
            if True not in data_dictionary:
                report.append(res_id)
    return report


def HTML_report_generate(resource_list): 
    table_row = ""
    for res_id in resource_list:
        if res_id:
            resource = get_resource_detail(res_id)
            resource_link = urljoin(CKAN_URL,"".join(["/dataset/", resource["package_id"],"/resource/", resource["id"]]))
            table_row +=  u"<tr><td><a href=\"{link}\">{name}</a></td><td>{state}</td></tr>".format(
                            link=resource_link.format(), name=resource["name"], state="Empty" ) 
               
    html = """\  
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                html {
                background : #f7f7f7;
                }
                body {
                width: 80%;
                margin: auto;
                background : #fff;
                padding: 40px;
                }
                .header {
                margin-top: 20px;
                background : #f7f7f7;
                padding : 20px 30px;
                }
                table {
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
            <div class="header">
                <img src ="$logo" height="40px"/>
            </div>
            <p><strong>Date Generated:</strong> $date 
            <p>
            <h3>List of the resource with empty data dictionaries.</h3>
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
    return Template(html).safe_substitute(
        table_row=table_row, 
        date=timezone.utcnow().strftime("%d-%b-%Y %H:%M:%S"),
        logo=LOGO)

def dispatch_email(**context):
    resource_list =  context["task_instance"].xcom_pull(task_ids="check_empty_data_dictionary")
    if resource_list: 
        message = Mail(
            from_email=MAIL_FROM,
            to_emails=[To("{0}".format(recipient)) for recipient in MAIL_TO.split(',')],
            subject="Empty data dictionary report.",
            html_content= HTML_report_generate(resource_list))
        try:
            sg = SendGridAPIClient(SENDGRID_KEY)
            response = sg.send(message)
            logging.info("{0} - Report successfully sent via email.".format(response.status_code))
        except Exception as e:
            logging.info(e.message)
    else:
        logging.info("Nothing to sent on report.")


check_empty_data_dictionary_task = PythonOperator(
    task_id="check_empty_data_dictionary",
    provide_context=True,
    python_callable=check_empty_data_dictionary,
    dag=dag,
)

dispatch_email_task = PythonOperator(
    task_id="dispatch_email",
    provide_context=True,
    python_callable=dispatch_email,
    dag=dag,
)

check_empty_data_dictionary_task  >> dispatch_email_task
