from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import time
import json
from uuid import uuid4
import os
from load import create_s3_client, create_s3_upload_data, get_s3_object, upload_to_S3

import requests

BUCKET_NAME = "test-bucket"

S3_CONFIG = {
    "aws_access_key_id": "_placeholder",
    "aws_secret_access_key": "_placeholder",
    "region_name": "us-east-1",
    "endpoint_url": "http://127.0.0.1:4566",
}

S3_CLIENT = create_s3_client(S3_CONFIG)
FILEKEY = str(uuid4())

"""
    - test passing data via xcom for all tasks
    - don't worry about s3 for right now
"""


@dag(
    "datagov_etl_pipeline",
    start_date=datetime(year=2023, month=2, day=5),
    catchup=False,
    tags=["etl"],
    schedule_interval=timedelta(minutes=2),
)
def etl_pipeline():
    @task(task_id="extract_dcatus")
    def extract():
        url = "https://data.ny.gov/api/views/5xaw-6ayf/rows.json?accessType=DOWNLOAD"
        res = requests.get(url)
        if res.status_code == 200:
            dcatus = res.json()
            return dcatus["dataset"]

        # try:
        #     catalog = download_json(job_info["url"])
        # except Exception as e:
        #     # do something with e
        #     return e

        # # check schema
        # if not is_dcatus_schema(catalog):
        #     return "invalid dcatus catalog"

        # # parse catalog and upload records
        # try:
        #     for record_info in parse_catalog(catalog, job_info):
        #         upload_to_S3(S3_client, record_info)
        #         output["s3_paths"].append(record_info["Key"])
        # except Exception as e:
        #     return False

        # return output

        return

    @tast(task_id="validate_dcatus")
    def validate():
        # success = None
        # error_message = ""

        # validator = Draft202012Validator(dataset_schema)

        # try:
        #     validator.validate(json_data)
        #     success = True
        #     error_message = "no errors"
        # except ValidationError:
        #     success = False
        #     errors = validator.iter_errors(json_data)
        #     error_message = parse_errors(errors)

        # return success, error_message

        return

    @task(task_id="transform_dcatus")
    def transform():
        return

    @task(task_id="load_dcatus")
    def load():
        dcatus = get_s3_object(S3_CLIENT, BUCKET_NAME, FILEKEY)
        for idx, record in enumerate(dcatus["dataset"]):
            upload_data = create_s3_upload_data(
                json.dumps(record), BUCKET_NAME, FILEKEY + str(idx), "application/json"
            )
            upload_to_S3(S3_CLIENT, upload_data)

    # extract() >> transform() >> load()

    list_filenames = extract()
    validated = list_filenames.output.map(validate)
    validated.output.map(load)


_ = etl_pipeline()
