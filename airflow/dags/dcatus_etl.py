from datetime import datetime
from airflow.decorators import dag, task
import json
from pathlib import Path
from jsonschema import Draft202012Validator
import requests

ROOT_DIR = Path(__file__).parents[2]
SCHEMA_DIR = ROOT_DIR / "schemas"
CATALOG_SCHEMA = SCHEMA_DIR / "dcatus" / "catalog.json"
DATASET_SCHEMA = SCHEMA_DIR / "dcatus" / "dataset.json"

with open(DATASET_SCHEMA) as json_file:
    dcatus_dataset_schema = json.load(json_file)

daily_data_url = "https://catalog.data.gov/api/action/package_search?fq=dataset_type:harvest%20AND%20source_type:datajson%20AND%20frequency:DAILY&rows=1000"
res = requests.get(daily_data_url).json()

for harvest_source in res["result"]["results"]:
    frequency = harvest_source["frequency"]
    title = "_".join(map(str.lower, harvest_source["title"].split()))
    url = harvest_source["url"]
    dag_id = f"{title}_workflow"

    @dag(
        dag_id,
        start_date=datetime(year=2023, month=8, day=26),
        schedule_interval=f"@{frequency.lower()}",
        catchup=False,
    )
    def etl_pipeline():
        def on_failure_callback(context):
            return context

        @task(task_id="extract_dcatus", on_failure_callback=on_failure_callback)
        def extract(url):
            return requests.get(url).json()["dataset"]

        @task(task_id="validate_dcatus", on_failure_callback=on_failure_callback)
        def validate(dcatus_record):
            validator = Draft202012Validator(dcatus_dataset_schema)
            validator.validate(dcatus_record)
            return dcatus_record

        @task(task_id="load_dcatus", on_failure_callback=on_failure_callback)
        def load(dcatus_record, ti=None):
            return ti.xcom_pull(task_ids="validate_dcatus")

        load.expand(dcatus_record=validate.expand(dcatus_record=extract(url)))

    globals()[dag_id] = etl_pipeline()
