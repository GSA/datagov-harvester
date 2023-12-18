from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import harvester

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple Airflow DAG for hello world',
    schedule_interval=timedelta(days=1),  # Set the frequency of DAG runs
)

def hello_world():
    print("Hello, World!")

task_hello_world = PythonOperator(
    task_id='hello_world_task',
    python_callable=hello_world,
    dag=dag,
)

def extract():
    harvester.extract("some-test-string")

task_extract = PythonOperator(
    task_id='custom_logic_task',
    python_callable=extract,  # Replace with your actual function
    dag=dag,
)

task_hello_world >> task_extract

