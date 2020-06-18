import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='csv_to_json',
    default_args=args,
    schedule_interval=None,
    tags=['conversion']
)


def convert(input, output, **kwargs):
    print('Starting file conversion')
    df = pd.read_csv(input)
    df.to_json(output, orient='records')
    print('End file conversion')


convert_task = PythonOperator(
    task_id="convert_to_json",
    provide_context=True,
    python_callable=convert,
    op_kwargs={'input': "../example.csv",
    'output': "../aircan-example-1.json"},
    dag=dag
)
