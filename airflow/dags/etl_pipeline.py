from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import time
import json

# from uuid import uuid4
from pathlib import Path
from jsonschema import Draft202012Validator
import requests

# s3 stuff

# BUCKET_NAME = "test-bucket"
# S3_CONFIG = {
#     "aws_access_key_id": "_placeholder",
#     "aws_secret_access_key": "_placeholder",
#     "region_name": "us-east-1",
#     "endpoint_url": "http://127.0.0.1:4566",
# }
# S3_CLIENT = create_s3_client(S3_CONFIG)
# FILEKEY = str(uuid4())

"""
    - test passing data via xcom for all tasks
    - don't worry about s3 for right now
"""

ROOT_DIR = Path(__file__).parents[2]
SCHEMA_DIR = ROOT_DIR / "schemas"
CATALOG_SCHEMA = SCHEMA_DIR / "catalog.json"
DATASET_SCHEMA = SCHEMA_DIR / "dataset.json"

with open(DATASET_SCHEMA) as json_file:
    dcatus_dataset_schema = json.load(json_file)


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
        url = "https://github.com/GSA/ckanext-datajson/blob/main/ckanext/datajson/tests/datajson-samples/large-spatial.data.json"
        # url = "https://github.com/GSA/ckanext-datajson/blob/main/ckanext/datajson/tests/datajson-samples/ny.data.json"
        res = requests.get(url)
        if res.status_code == 200:
            data = res.json()["payload"]["blob"]["rawLines"]
            return json.loads("".join(data))["dataset"]

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

    @task(task_id="validate_dcatus")
    def validate(dcatus_record):
        validator = Draft202012Validator(dcatus_dataset_schema)
        try:
            validator.validate(dcatus_record)
            return dcatus_record
        except:
            return False

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

    # list_filenames = extract()
    # validated = list_filenames.output.map(validate)

    validate.expand(dcatus_record=extract())

    # validated.output.map(load)


_ = etl_pipeline()
