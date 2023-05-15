from .utils import download_catalog
import json
from datagovharvester.utils.s3_utilities import (
    upload_to_S3,
    create_s3_payload,
)


def extract_json_catalog(url, job_id, S3_client, bucket_name):
    errors = []

    try:
        data = download_catalog(url)
    except Exception as e:
        # do something with the error
        return e

    if not data.get("dataset", None):
        # do something with the error
        return "no dataset key"

    for idx, record in enumerate(data["dataset"]):
        try:
            record = json.dumps(record)
            key_name = f"{job_id}_{idx}_extract.json"
            s3_payload = create_s3_payload(record, bucket_name, key_name)
            upload_to_S3(S3_client, s3_payload)
        except Exception as e:
            # do something with the error
            errors.append(e)
            continue

    return errors
