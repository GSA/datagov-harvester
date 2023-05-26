import boto3
import botocore

# ruff: noqa: F841

def create_s3_client(s3_config):
    """ create boto3.client object
    s3_config (dict)    :   configuration dict. 
    """
    try:
        return boto3.client("s3", **s3_config)
    except botocore.exceptions.ClientError as e: #noqa
        pass 

def create_s3_payload(json_str, bucket_name, key_name):
    """ create s3 payload to be added to the input bucket
    json_str (str)      :   data to be placed in s3 bucket as json string. 
    bucket_name (str)   :   name of bucket to store the data. 
    key_name (str)      :   name of the file to be placed in the s3 bucket. 
    """
    return {
        "Body": json_str,
        "Bucket": bucket_name,
        "Key": key_name,
        "ContentType": "application/json",
    }


def upload_to_S3(S3, s3_payload):
    """ store the s3 payload 
    S3 (boto3 client)   :   boto3 S3 client
    s3_payload (dict)   :   payload to be stored in s3 bucket.
    """
    try:
        return S3.put_object(**s3_payload)
    except Exception as e:
        pass 