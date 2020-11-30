import logging
import datetime
import time
from pprint import pprint

## for the logging to identify each dag,task and run
import json
import copy

#just for some random number
import random
# Third-party library imports
from airflow import DAG
from airflow.exceptions import AirflowException

from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import uuid

default_args = {"owner": "airflow", 
                "start_date": days_ago(0),
}

def _log_context(**context):
    log_id = {
        # 'task_instance_key_str': context['task_instance_key_str'],
        'run_id': context['run_id']
    }
    
    my_log = copy.deepcopy(log_id)
    my_log['message': f"This is a log message:  {random.random()}"]
    logging.info(json.dumps(my_log))
    pprint(4+4)  # nothing, just some thing to put here
    time.sleep(0.01)
    return f"Log test return : {json.dumps(log_id)}"

   

def sleep_foo(**kwargs):
    pprint("Context as KWARGS: " + str(kwargs))
    log_id = {
        # 'task_instance_key_str': kwargs['task_instance_key_str'],
        'run_id': kwargs['run_id']
    }
    my_log = copy.deepcopy(log_id)
    my_log['message': "This is my sleep message:  I'm sleeping!"]
    logging.info(json.dumps(my_log))
    time.sleep(0.1)
    return f"Back from Sleep: {json.dumps(log_id)}"


dag =  DAG(dag_id="example_logs", 
           default_args=default_args, 
           schedule_interval=None, 
           tags=['example']
           )

dodo = PythonOperator(
    task_id="dodo",
    provide_context=True,
    python_callable=sleep_foo,
    dag=dag
)

log_context_1 = PythonOperator(
    task_id="log_context_1",
    provide_context=True,
    python_callable=_log_context,
    dag=dag
)
    
log_context_and_parms_1 = PythonOperator(
    task_id="log_context_and_parms_1",
    provide_context=True,
    python_callable=_log_context,
    dag=dag
)

log_context_1 >> dodo >> log_context_and_parms_1
