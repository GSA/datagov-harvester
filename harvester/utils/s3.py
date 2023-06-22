import boto3

from harvester import bucket_name

# ruff: noqa: F841


def create_s3_client(s3_config):
    """create boto3.client object
    s3_config (dict)    :   configuration dict.
    """
    try:
        return boto3.client("s3", **s3_config)
    except ValueError as e:
        return e


def create_s3_upload_data(body, key_name, content_type):
    """create s3 data to be uploaded to the default bucket
    json_str (str)      :   data to be placed in s3 bucket as json string.
    key_name (str)      :   name of the file to be placed in the s3 bucket.
    """
    return {
        "Body": body,
        "Bucket": bucket_name,
        "Key": key_name,
        "ContentType": content_type,
    }

def delete_s3_object( S3, bucket_name, object_key ):

    try:
        return S3.delete_object( Bucket=bucket_name, Key=object_key )
    except Exception as e:
        pass 
    
def get_s3_object( S3, bucket_name, object_key ):

    try:
        return S3.get_object( Bucket=bucket_name, Key=object_key )
    except Exception as e:
        pass

def upload_to_S3(S3, s3_upload_data):
    """store the s3 payload
    S3 (boto3 client)   :   boto3 S3 client
    s3_upload_data (dict)   :   payload to be stored in s3 bucket.
    """
    try:
        return S3.put_object(**s3_upload_data)
    except Exception as e:
        pass
