from .utils import download_catalog
import json
from datagovharvester.utils.s3_utilities import (
    upload_to_S3,
    create_s3_payload,
)

def store_record_in_s3( record, source_id, job_id, record_idx, S3_client, bucket_name ):
    """ store the input record in a s3 bucket
    record (dict)               :   dcatus record
    source_id (str)             :   uuid 
    job_id (str)                :   uuid
    record_idx (int)            :   index position of record in records list
    S3_client (boto3.client)    :   boto3 s3 client. 
    bucket_name (str)           :   name of bucket to store data. 
    """
    try:
        record = json.dumps(record)
        key_name = f"{source_id}/{job_id}/{record_idx}.json"
        s3_payload = create_s3_payload(record, bucket_name, key_name)
        upload_to_S3(S3_client, s3_payload)
        return key_name
    except Exception as e:
        return e

def extract_json_catalog(url, source_id, job_id, S3_client, bucket_name):
    """ download, extract, and store dcatus records in s3 bucket
    url (str)                   :   download file path 
    source_id (str)             :   uuid 
    job_id (str)                :   uuid
    S3_client (boto3.client)    :   boto3 s3 client. 
    bucket_name (str)           :   name of bucket to store data. 
    """

    output = { "errors": [], "job_ids": [], "s3_paths": [] } 

    try:
        data = download_catalog(url)
    except Exception as e:
        # do something with the error
        return e

    if not data.get("dataset", None):
        # do something with the error
        return "no dataset key"

    for idx, record in enumerate(data["dataset"]):
        try:
            store_record_in_s3( record, source_id, job_id, idx, S3_client, bucket_name )
            s3_path = bucket_name + f"/{source_id}/{job_id}/{idx}.json"
            output["s3_paths"].append( s3_path ) 
            output["job_ids"].append( job_id ) 
        except Exception as e:
            output["errors"].append( e ) 

    return output
