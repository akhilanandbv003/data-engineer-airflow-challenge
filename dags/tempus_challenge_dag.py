from datetime import datetime, timedelta

import challenge.runner as rx
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 22),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Object
dag = DAG(
    'dag001',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),  # DAG will run once every 5 minutes
    catchup=False,
)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

task001 = PythonOperator(task_id='task001', python_callable=rx.api_to_s3, dag=dag)

dummy_operator >> task001
