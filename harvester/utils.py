import hashlib
import json
import os
import string
import random

import boto3
import sansjson

# ruff: noqa: F841


def create_random_text(str_len: int) -> str:

    alphabet = string.ascii_lowercase
    output = ""

    for _ in range(str_len):
        output += alphabet[random.randint(0, len(alphabet))]
    return output


def convert_set_to_list(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


def sort_dataset(d):
    return sansjson.sort_pyobject(d)


def dataset_to_hash(d):
    return hashlib.sha256(json.dumps(d, sort_keys=True).encode("utf-8")).hexdigest()


def open_json(file_path):
    """open input json file as dictionary
    file_path (str)     :   json file path.
    """
    with open(file_path) as fp:
        return json.load(fp)


class S3Handler:
    def __init__(self):
        self.access_key_id = os.getenv("S3FILESTORE__AWS_ACCESS_KEY_ID")
        self.secret_access_key = os.getenv("S3FILESTORE__AWS_SECRET_ACCESS_KEY")
        self.region_name = os.getenv("S3FILESTORE__REGION_NAME")
        self.endpoint_url = os.getenv("S3FILESTORE__HOST_NAME")
        self.bucket = os.getenv("S3FILESTORE__AWS_BUCKET_NAME")

        # S3://{h20-bucket-prefix}/{harvest-source}/{harvest-job-id}/{ETL-process-step}/{example.json}
        self.out_harvest_source = "{}/{}/{}/{ETL-process-step}/{example.json}"
        self.out_harvest_record = None

        self.config = {
            "aws_access_key_id": self.access_key_id,
            "aws_secret_access_key": self.secret_access_key,
            "region_name": self.region_name,
            "endpoint_url": self.endpoint_url,
        }

        self.client = boto3.client("s3", **self.config)

    def create_bucket(self):
        return self.client.create_bucket(Bucket=self.bucket)

    def delete_object(self, object_key: str):
        return self.client.delete_object(Bucket=self.bucket, Key=object_key)

    def get_object(self, object_key: str):
        return self.client.get_object(Bucket=self.bucket, Key=object_key)

    def put_object(self, body: str, key_name: str):
        return self.client.put_object(
            **{
                "Body": body,
                "Bucket": self.bucket,
                "Key": key_name,
                "ContentType": "application/json",
            }
        )
