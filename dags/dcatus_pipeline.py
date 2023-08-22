import json
import pendulum
import requests

from pathlib import Path
from airflow import Dataset
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from includes.validate.dcat_us import validate_json_schema
from includes.utils.json_util import open_json

SRC = Dataset(
    "https://raw.githubusercontent.com/GSA/catalog.data.gov/main/tests/harvest-sources/data.json"
)
now = pendulum.now()


def make_path(fileName):
    return Path(__file__).parents[1] / "data" / "schemas" / fileName


@dag(
    "dcatus_pipeline",
    start_date=datetime(year=2023, month=2, day=5),
    catchup=False,
    tags=["etl"],
    schedule_interval=timedelta(minutes=2),
)
def dcatus_pipeline():
    @task(task_id="extract_dcatus")
    def extract(src: Dataset) -> list:
        resp = requests.get(url=src.uri)
        data = resp.json()
        print(data)
        return data["dataset"]

    @task(task_id="validate_dcatus")
    def validate(dataset: Dataset) -> Dataset:
        file_path = make_path("dataset.json")
        print("file path ::: {file_path}")
        dataset_schema = open_json(file_path)
        [success, error] = validate_json_schema(dataset, dataset_schema)
        print(f"success: {success}, error: {error}")
        return True if success else False

    dataset = extract(SRC)
    validated = validate.expand(dataset=dataset)


dcatus_pipeline()
