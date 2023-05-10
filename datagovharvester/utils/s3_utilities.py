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


def create_s3_bucket(S3, bucket_name):
    error_message = None

    try:
        S3.create_bucket(Bucket=bucket_name)
    except Exception as e:
        error_message = e

    return bucket_name, error_message


def create_s3_payload(json_str, bucket_name, key_name):
    return {
        "Body": json_str,
        "Bucket": bucket_name,
        "Key": key_name,
        "ContentType": "application/json",
    }


def upload_dcatus_to_S3(S3, s3_payload):
    output = None
    error_message = None

    try:
        output = S3.put_object(**s3_payload)
    except Exception as e:
        error_message = e

    return output, error_message
