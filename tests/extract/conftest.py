import pytest
from dotenv import load_dotenv
import os
from uuid import uuid4
from datagovharvester.utils.s3_utilities import create_s3_client


@pytest.fixture
def get_dcatus_job():
    """example dcatus job payload"""
    return {
        "url": "http://localhost/dcatus.json",
        "source_id": str(uuid4()),
        "job_id": str(uuid4()),
    }

@pytest.fixture
def get_bad_url():
    """example dcatus job payload with bad url"""
    return {
        "url": "http://localhost/bad_url",
        "source_id": str(uuid4()),
        "job_id": str(uuid4()),
    }

@pytest.fixture
def get_bad_json():
    """example bad json with missing enclosing bracket"""
    return {
        "url": "http://localhost/unclosed.json",
        "source_id": str(uuid4()),
        "job_id": str(uuid4()),
    }

@pytest.fixture
def get_no_dataset_key_dcatus_json():
    """example dcatus json with no 'dataset' key"""
    return {
        "url": "http://localhost/no_dataset_key.json",
        "source_id": str(uuid4()),
        "job_id": str(uuid4()),
    }

@pytest.fixture
def create_client_config():
    """create s3 configuration dictionary intended 
    to be passed to boto3.client("s3", **s3_config)"""
    config = {}
    load_dotenv()

    config["aws_access_key_id"] = os.getenv("S3FILESTORE__AWS_ACCESS_KEY_ID")
    config["aws_secret_access_key"] = os.getenv("S3FILESTORE__AWS_SECRET_ACCESS_KEY")
    config["region_name"] = os.getenv("S3FILESTORE__REGION_NAME")
    config["endpoint_url"] = os.getenv("S3FILESTORE__HOST_NAME")

    return config


@pytest.fixture
def create_client(create_client_config):
    """create a boto3.client
    create_client_config (dict)     :   configuration file.
    """
    return create_s3_client(create_client_config)
