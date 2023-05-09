import boto3
import botocore


def create_s3_client(s3_config):
    S3 = None
    error_message = None
    try:
        S3 = boto3.client("s3", **s3_config)
    except botocore.exceptions.ClientError as e:
        error_message = e

    return S3, error_message


def create_or_get_bucket(S3, bucket_name):
    error_message = None

    try:
        resp = S3.list_buckets()
        if bucket_name not in [bucket["Name"] for bucket in resp["Buckets"]]:
            S3.create_bucket("test-bucket")
    except Exception as e:
        error_message = e

    return bucket_name, error_message


def create_S3_payload(body, bucket_name, key_name, content_type):
    return {
        "Body": body,
        "Bucket": bucket_name,
        "Key": key_name,
        "ContentType": content_type,
    }


def upload_dcatus_to_S3(S3, json_str, bucket_name, key_name):
    output = None
    error_message = None

    config = {
        "Body": json_str,
        "Bucket": bucket_name,
        "Key": key_name,
        "ContentType": "application/json",
    }

    try:
        output = S3.put_object(**config)
    except Exception as e:
        error_message = e

    return output, error_message
