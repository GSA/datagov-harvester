import hashlib
import json
import os

import boto3
import sansjson

from cloudfoundry_client.client import CloudFoundryClient
from cloudfoundry_client.v3.tasks import TaskManager

# ruff: noqa: F841


def convert_set_to_list(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError


def sort_dataset(d):
    return sansjson.sort_pyobject(d)


def dataset_to_hash(d):
    # TODO: check for sh1 or sha256?
    # https://github.com/GSA/ckanext-datajson/blob/a3bc214fa7585115b9ff911b105884ef209aa416/ckanext/datajson/datajson.py#L279
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


class CFHandler:
    def __init__(self, url: str = None, user: str = None, password: str = None):
        self.target_endpoint = url if url is not None else os.getenv("CF_API_URL")
        self.client = CloudFoundryClient(self.target_endpoint)
        self.client.init_with_user_credentials(
            user if user is not None else os.getenv("CF_SERVICE_USER"),
            password if password is not None else os.getenv("CF_SERVICE_AUTH"),
        )

        self.task_mgr = TaskManager(self.target_endpoint, self.client)

    def start_task(self, app_guuid, command, task_id):
        return self.task_mgr.create(app_guuid, command, task_id)

    def stop_task(self, task_id):
        return self.task_mgr.cancel(task_id)

    def get_task(self, task_id):
        return self.task_mgr.get(task_id)

    def get_all_app_tasks(self, app_guuid):
        return [task for task in self.client.v3.apps[app_guuid].tasks()]

    def get_all_running_tasks(self, tasks):
        return sum(1 for _ in filter(lambda task: task["state"] == "RUNNING", tasks))

    def read_recent_app_logs(self, app_guuid, task_id=None):

        app = self.client.v2.apps[app_guuid]
        logs = filter(lambda lg: task_id in lg, [str(log) for log in app.recent_logs()])
        return "\n".join(logs)
