import requests
import json
import os
from datagovharvester.utils.s3_utilities import (
    upload_dcatus_to_S3,
    create_s3_client,
    create_bucket,
    create_s3_payload,
)


def download_catalog(url):
    resp = None
    error = None

    try:
        resp = requests.get(url)
    except requests.exceptions.RequestException as e:
        error = e

    return resp, error


def fetch_url(url):
    success = False

    resp, error_msg = download_catalog(url)

    if resp.status_code == 200:
        success = True

    data = resp.json()

    return data, success


def extract_catalog(url, S3_config, job_id):
    data, fetch_success = fetch_url(url)

    S3, S3_create_error_msg = create_s3_client(S3_config)

    bucket_name = os.getenv("S3FILESTORE__AWS_BUCKET_NAME")
    bucket_name, bucket_error_msg = create_bucket(S3, bucket_name)

    for idx, record in enumerate(data["dataset"]):
        record = json.dumps(record)
        key_name = f"{job_id}_{idx}_extract.json"
        s3_payload = create_s3_payload(record, bucket_name, key_name)

        upload_data, upload_error_message = upload_dcatus_to_S3(S3, s3_payload)

    return upload_data, upload_error_message
