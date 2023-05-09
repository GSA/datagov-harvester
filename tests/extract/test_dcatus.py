from datagovharvester.extract.utils import extract_data
from datagovharvester.utils.s3_utilities import (
    upload_dcatus_to_S3,
    create_s3_client,
    create_or_get_bucket,
)
import json
import os


def test_extract_dcatus(create_s3_client_config, get_dcatus_job):
    success = False

    url = get_dcatus_job["url"]
    source_id = get_dcatus_job["source_id"]

    resp, error_msg = extract_data(url)

    if resp.status_code != 200:
        assert success

    json_data = resp.json()
    json_str = json.dumps(json_data)

    S3_config = create_s3_client_config
    S3, S3_create_error_msg = create_s3_client(S3_config)

    bucket_name = os.getenv("S3FILESTORE__AWS_BUCKET_NAME")
    bucket_name, bucket_error_msg = create_or_get_bucket(S3, bucket_name)

    key_name = source_id + "_extract.json"

    res, final_error = upload_dcatus_to_S3(S3, json_str, bucket_name, key_name)

    if final_error is None:
        success = True

    assert success
