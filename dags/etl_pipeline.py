import json
from datetime import datetime
from pathlib import Path

import requests
from airflow.decorators import dag, task
from jsonschema import Draft202012Validator


CURRENT_DIR = Path(__file__).parent.absolute()
CATALOG_SCHEMA = CURRENT_DIR / "schemas/catalog.json"
DATASET_SCHEMA = CURRENT_DIR / "schemas/dataset.json"

DAILY_HARVEST_SOURCE = CURRENT_DIR / "sources/daily.json"

def create_dag(dag_id, schedule, default_args, tags, url):
    @dag(dag_id=dag_id, schedule=schedule, tags=tags, default_args=default_args, catchup=False)
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

        extracted_record=extract(url)
        validated_record=validate.expand(dcatus_record=extracted_record)
        load.expand(dcatus_record=validated_record)
    
    generated_dag = etl_pipeline()

    return generated_dag


with DAILY_HARVEST_SOURCE.open() as fp:
    res = json.load(fp)
    sources = res['result']['results']
    for source in sources:
        title = source['name']
        url = source['url']
        dag_id = f"{title}_workflow"
        default_args = {"owner": "airflow", "start_date": datetime(2023, 7, 1)}
        schedule = "@daily"
        tags = ['daily']

        create_dag(dag_id, schedule, default_args, tags, url)
