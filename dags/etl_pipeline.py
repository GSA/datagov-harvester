import json
from datetime import datetime
from pathlib import Path

import requests
from airflow.decorators import dag, task
from jsonschema import Draft202012Validator

ROOT_DIR = Path(__file__).parents[1]
SCHEMA_DIR = ROOT_DIR.joinpath("dags").joinpath("schemas")
CATALOG_SCHEMA = SCHEMA_DIR.joinpath("catalog.json")
DATASET_SCHEMA = SCHEMA_DIR.joinpath("dataset.json")

def create_dag(dag_id, schedule, default_args, url):
    @dag(dag_id=dag_id, schedule=schedule, default_args=default_args, catchup=False)
    def etl_pipeline():
        def on_failure_callback(context):
            ti = context['task_instance']
            print(f"task {ti.task_id } failed in dag { ti.dag_id } ")

        @task(task_id="extract_dcatus", on_failure_callback=on_failure_callback)
        def extract(*args):
            return requests.get(url).json()["dataset"]

        @task(task_id="validate_dcatus", on_failure_callback=on_failure_callback)
        def validate(dcatus_record):
            with open(DATASET_SCHEMA) as json_file:
                dcatus_dataset_schema = json.load(json_file)
            validator = Draft202012Validator(dcatus_dataset_schema)
            validator.validate(dcatus_record)
            return dcatus_record

        @task(task_id="load_dcatus", trigger_rule='all_done', on_failure_callback=on_failure_callback)
        def load(dcatus_record, ti=None):
            return ti.xcom_pull(task_ids="validate_dcatus")

        load.expand(dcatus_record=validate.expand(dcatus_record=extract(url)))
    
    generated_dag = etl_pipeline()

    return generated_dag


sources = [
    {'name': 'fcc',
     'url': 'https://opendata.fcc.gov/data.json'}
]

for n in sources:
    title = n['name']
    url = n['url']
    dag_id = f"{title}_workflow"
    default_args = {"owner": "airflow", "start_date": datetime(2023, 7, 1)}
    schedule = "@daily"

    create_dag(dag_id, schedule, default_args, url)
