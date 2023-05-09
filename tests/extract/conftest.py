import pytest
from dotenv import load_dotenv
import os
from uuid import uuid4


@pytest.fixture
def get_dcatus_job():
    return {
        "url": "http://localhost:80/data.json",
        "source_id": str(uuid4()),
        "job_id": str(uuid4()),
    }


@pytest.fixture
def create_s3_client_config():
    config = {}
    load_dotenv()

    config["aws_access_key_id"] = os.getenv("S3FILESTORE__AWS_ACCESS_KEY_ID")
    config["aws_secret_access_key"] = os.getenv("S3FILESTORE__AWS_SECRET_ACCESS_KEY")
    config["region_name"] = os.getenv("S3FILESTORE__REGION_NAME")
    config["endpoint_url"] = os.getenv("S3FILESTORE__HOST_NAME")

    return config
