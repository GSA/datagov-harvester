from harvester.extract.dcatus import parse_catalog
from harvester.utils.s3 import upload_to_S3
from harvester.utils.json import download_json
from harvester.validate.dcat_us import is_dcatus_schema

# ruff: noqa: F841


def main(job_info, S3_client):
    """extract a file, mild validation, upload to s3 bucket.
    job_info (dict)             :   info on the job ( e.g. source_id, job_id, url )
    S3_client (boto3.client)    :   S3 client
    """
    output = {"job_id": job_info["job_id"], "s3_paths": []}

    # download file
    try:
        catalog = download_json(job_info["url"])
    except Exception as e:
        # do something with e
        return e

    # check schema
    if not is_dcatus_schema(catalog):
        return "invalid dcatus catalog"

    # parse catalog and upload records
    try:
        for record_info in parse_catalog(catalog, job_info):
            upload_to_S3(S3_client, record_info)
            output["s3_paths"].append(record_info["Key"])
    except Exception as e:
        return False

    return output
