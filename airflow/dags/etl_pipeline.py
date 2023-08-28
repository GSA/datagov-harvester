from datetime import datetime
from airflow.decorators import dag, task
import json
from pathlib import Path
from jsonschema import Draft202012Validator
import requests

ROOT_DIR = Path(__file__).parents[2]
SCHEMA_DIR = ROOT_DIR / "schemas"
CATALOG_SCHEMA = SCHEMA_DIR / "catalog.json"
DATASET_SCHEMA = SCHEMA_DIR / "dataset.json"

with open(DATASET_SCHEMA) as json_file:
    dcatus_dataset_schema = json.load(json_file)

daily_data_url = "https://catalog.data.gov/api/action/package_search?fq=dataset_type:harvest%20AND%20source_type:datajson%20AND%20frequency:DAILY&rows=1000"
res = requests.get(daily_data_url).json()

for harvest_source in res["result"]["results"]:
    frequency = harvest_source["frequency"]
    title = "_".join(map(str.lower, harvest_source["title"].split()))
    url = harvest_source["url"]

    @dag(
        f"{title}_workflow",
        start_date=datetime(year=2023, month=2, day=5),
        schedule_interval=f"@{frequency.lower()}",
    )
    def etl_pipeline():
        @task(task_id="extract_dcatus")
        def extract(url):
            return requests.get(url).json()

        @task(task_id="validate_dcatus")
        def validate(dcatus_record):
            validator = Draft202012Validator(dcatus_dataset_schema)
            validator.validate(dcatus_record)
            return dcatus_record

        @task(task_id="load_dcatus")
        def load(dcatus_record, ti=None):
            print("grabbing validation catalog via xcom")
            return ti.xcom_pull(task_ids="validate_dcatus")

        # @task(task_id="extract_error_handler")
        # def extract_error_handler(ti=None):
        #     res = ti.xcom_pull(task_ids="extract_dcatus")
        #     print(res)

        # extract() >> extract_error_handler()

        load.expand(dcatus_record=validate.expand(dcatus_record=extract(url)))

    _ = etl_pipeline()
