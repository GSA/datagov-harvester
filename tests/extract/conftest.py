import pytest
from dotenv import load_dotenv
import os
from uuid import uuid4
from datagovharvester.utils.s3_utilities import create_s3_client, create_s3_bucket


@pytest.fixture
def get_dcatus_job():
    return {
        "url": "http://localhost/data.json",
        "source_id": str(uuid4()),
        "job_id": str(uuid4()),
    }


@pytest.fixture
def create_client_config():
    config = {}
    load_dotenv()

    config["aws_access_key_id"] = os.getenv("S3FILESTORE_AWS_ACCESS_KEY_ID")
    config["aws_secret_access_key"] = os.getenv("S3FILESTORE_AWS_SECRET_ACCESS_KEY")
    config["region_name"] = os.getenv("S3FILESTORE_REGION_NAME")
    config["endpoint_url"] = os.getenv("S3FILESTORE_HOST_NAME")

    return config


@pytest.fixture
def create_client(create_client_config):
    return create_s3_client(create_client_config)


@pytest.fixture
def create_bucket(create_client):
    s3_client, s3_client_msg = create_client
    return create_s3_bucket(s3_client, "test-bucket")
