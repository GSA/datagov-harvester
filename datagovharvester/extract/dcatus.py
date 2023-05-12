from .utils import fetch_url
import json
from datagovharvester.utils.s3_utilities import (
    upload_to_S3,
    create_s3_payload,
)


def extract_dcatus_catalog(url, job_id, S3_client, bucket_name):
    extracted_data = []
    error_message = None

    try:
        data, fetch_success = fetch_url(url)

        for idx, record in enumerate(data["dataset"]):
            record = json.dumps(record)
            key_name = f"{job_id}_{idx}_extract.json"
            s3_payload = create_s3_payload(record, bucket_name, key_name)

            upload_data, upload_error_message = upload_to_S3(S3_client, s3_payload)
            extracted_data.append([upload_data, upload_error_message])
    except Exception as e:
        error_message = e

    return extracted_data, error_message
