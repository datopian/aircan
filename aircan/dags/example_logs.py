"""
This DAG file shows logging in python in a way to be able to identify the run_id of every task

Example log outputs are in order of task execution, and also each task has 2 logs corresponding to:
 - first log shows the output of logging.info(...)
 - second log shows the output of the automated logging done by AirFlow when receiving a return value

### Local Run example:

For task_id="log_context_1"

    [2020-11-30 17:13:01,624] {example_logs.py:56} INFO - {"task_instance_key_str": "example_logs__log_context_1__20201130", "run_id": "RUNID-SSS-1234567890", "message": "This is My Log Message..."}
    [2020-11-30 17:13:01,625] {python_operator.py:114} INFO - Done. Returned value was: {"task_instance_key_str": "example_logs__log_context_1__20201130", "run_id": "RUNID-SSS-1234567890", "message": "This is My Log Message..."}

For task_id="dodo"

    [2020-11-30 17:14:06,657] {example_logs.py:74} INFO - {"task_instance_key_str": "example_logs__dodo__20201130", "run_id": "RUNID-SSS-1234567890", "message": "I'm sleeping, let me rest..."}
    [2020-11-30 17:14:06,757] {python_operator.py:114} INFO - Done. Returned value was: {"task_instance_key_str": "example_logs__dodo__20201130", "run_id": "RUNID-SSS-1234567890", "message": "I'm sleeping, let me rest..."}


For task_id="log_context_and_parms_1"

    [2020-11-30 17:15:11,656] {example_logs.py:56} INFO - {"task_instance_key_str": "example_logs__log_context_and_parms_1__20201130", "run_id": "RUNID-SSS-1234567890", "message": "This is My Log Message..."}
    [2020-11-30 17:15:11,656] {python_operator.py:114} INFO - Done. Returned value was: {"task_instance_key_str": "example_logs__log_context_and_parms_1__20201130", "run_id": "RUNID-SSS-1234567890", "message": "This is My Log Message..."}

### GCP Composer Run example:

For task_id="log_context_1"

    [2020-11-30 16:39:09,647] {base_task_runner.py:115} INFO - Job 42030: Subtask log_context_1 [2020-11-30 16:39:09,647] {example_logs.py:60} INFO - {"task_instance_key_str": "example_logs__log_context_1__20201130", "run_id": "manual__2020-11-30T16:38:47.518321+00:00", "message": "This is My Log Message..."}@-@{"workflow": "example_logs", "task-id": "log_context_1", "execution-date": "2020-11-30T16:38:47.518321+00:00"}
    [2020-11-30 16:39:09,648] {base_task_runner.py:115} INFO - Job 42030: Subtask log_context_1 [2020-11-30 16:39:09,648] {python_operator.py:114} INFO - Done. Returned value was: {"task_instance_key_str": "example_logs__log_context_1__20201130", "run_id": "manual__2020-11-30T16:38:47.518321+00:00", "message": "This is My Log Message..."}@-@{"workflow": "example_logs", "task-id": "log_context_1", "execution-date": "2020-11-30T16:38:47.518321+00:00"}

For task_id="dodo"

    [2020-11-30 16:39:30,962] {base_task_runner.py:115} INFO - Job 42031: Subtask dodo [2020-11-30 16:39:30,962] {example_logs.py:78} INFO - {"task_instance_key_str": "example_logs__dodo__20201130", "run_id": "manual__2020-11-30T16:38:47.518321+00:00", "message": "I'm sleeping, let me rest..."}@-@{"workflow": "example_logs", "task-id": "dodo", "execution-date": "2020-11-30T16:38:47.518321+00:00"}
    [2020-11-30 16:39:31,070] {base_task_runner.py:115} INFO - Job 42031: Subtask dodo [2020-11-30 16:39:31,070] {python_operator.py:114} INFO - Done. Returned value was: {"task_instance_key_str": "example_logs__dodo__20201130", "run_id": "manual__2020-11-30T16:38:47.518321+00:00", "message": "I'm sleeping, let me rest..."}@-@{"workflow": "example_logs", "task-id": "dodo", "execution-date": "2020-11-30T16:38:47.518321+00:00"}

For task_id="log_context_and_parms_1"

    [2020-11-30 16:39:54,571] {base_task_runner.py:115} INFO - Job 42032: Subtask log_context_and_parms_1 [2020-11-30 16:39:54,571] {example_logs.py:60} INFO - {"task_instance_key_str": "example_logs__log_context_and_parms_1__20201130", "run_id": "manual__2020-11-30T16:38:47.518321+00:00", "message": "This is My Log Message..."}@-@{"workflow": "example_logs", "task-id": "log_context_and_parms_1", "execution-date": "2020-11-30T16:38:47.518321+00:00"}
    [2020-11-30 16:39:54,571] {base_task_runner.py:115} INFO - Job 42032: Subtask log_context_and_parms_1 [2020-11-30 16:39:54,571] {python_operator.py:114} INFO - Done. Returned value was: {"task_instance_key_str": "example_logs__log_context_and_parms_1__20201130", "run_id": "manual__2020-11-30T16:38:47.518321+00:00", "message": "This is My Log Message..."}@-@{"workflow": "example_logs", "task-id": "log_context_and_parms_1", "execution-date": "2020-11-30T16:38:47.518321+00:00"}



"""
import logging
import datetime
import time
from pprint import pprint

## for the logging to identify each dag,task and run
import json
import copy

# Third-party library imports
from airflow import DAG
from airflow.exceptions import AirflowException

from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {"owner": "airflow", 
                "start_date": days_ago(0),

}

# to identify the run the task needs to have hands on the context
def _log_context(**context):
    try:
        # for logging a message please do it this way, so the run is able to be identified
        log_id = {
            'task_instance_key_str': context['task_instance_key_str'],
            'run_id': context['run_id']
        }
        my_log = copy.deepcopy(log_id)
        my_log['message'] = "This is My Log Message..."
        jsn_log = json.dumps(my_log)
        logging.info(jsn_log)
        return jsn_log
    except:
        logging.error("context does not have run_id ")
    # logging.info("LOGGING context = ", context)
    # you can also implicitly log by returning a string here, if so, please use the same method as before
    return f"Log test returned without being able to create a nice message"
    

def sleep_foo(**kwargs):
    try:
        log_id = {
            'task_instance_key_str': kwargs['task_instance_key_str'],
            'run_id': kwargs['run_id']
        }
        my_log = copy.deepcopy(log_id)
        my_log['message'] = "I'm sleeping, let me rest..."
        jsn_log = json.dumps(my_log)
        logging.info(jsn_log)
        time.sleep(0.1)
        return jsn_log
    except:
        logging.error("context does not have run_id ")
        
    # pprint("Context as KWARGS: " + str(kwargs))
    # time.sleep(0.1)
    return f"Back from Sleep without message"


dag =  DAG(dag_id="example_logs", 
           default_args=default_args, 
           schedule_interval=None, 
           )

dodo = PythonOperator(
    task_id="dodo",
    provide_context=True,  # Needs this to be able to identify the run in the log
    python_callable=sleep_foo,
    dag=dag
)

log_context_1 = PythonOperator(
    task_id="log_context_1",
    provide_context=True,  # Needs this to be able to identify the run in the log
    python_callable=_log_context,
    dag=dag
)
    
log_context_and_parms_1 = PythonOperator(
    task_id="log_context_and_parms_1",
    provide_context=True,  # Needs this to be able to identify the run in the log
    python_callable=_log_context,
    dag=dag
)

log_context_1 >> dodo >> log_context_and_parms_1
