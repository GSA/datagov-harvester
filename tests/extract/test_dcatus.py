from pathlib import Path
from deepdiff import DeepDiff
import json 

from harvester import bucket_name
from harvester.extract import main
from harvester.utils.s3 import get_s3_object
from harvester.utils.json import open_json

TEST_DIR = Path(__file__).parents[1]

def test_extract_dcatus(get_dcatus_job, create_client):
    """download dcat-us json file and store result in s3 bucket.
    get_dcatus_job (dict)           :   fixture containing job data
    create_client  (boto3.client)   :   S3 client object
    """

    S3_client = create_client
    S3_client.create_bucket(Bucket=bucket_name)

    res = main(get_dcatus_job, S3_client)
    assert res 

    # check if the s3 record matches the test record
    dcatus_json = TEST_DIR / "harvest-sources" / "dcatus.json"
    dcatus_record_obj = open_json( dcatus_json )["dataset"][0]

    s3_record_obj = get_s3_object( S3_client, "test-bucket", res["s3_paths"][0] )
    s3_record_obj = s3_record_obj["Body"].read().decode("utf-8")
    s3_record_obj = json.loads( s3_record_obj ) 

    assert len( DeepDiff( dcatus_record_obj, s3_record_obj ) ) == 0 

def test_extract_bad_url(get_bad_url, create_client):
    """attempt to download a bad url.
    get_bad_url (dict)              :   fixture containing job data with bad url
    create_client  (boto3.client)   :   S3 client object
    """

    S3_client = create_client
    S3_client.create_bucket(Bucket=bucket_name)
 
    assert str(main(get_bad_url, S3_client)) == "non-200 status code"

    paginator = S3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket="test-bucket")

    for page in page_iterator:
        assert page["KeyCount"] == 0
    
def test_extract_bad_json(get_bad_json, create_client):
    """attempt to download a bad url.
    get_bad_json (dict)             :   fixture containing job data with bad json
    create_client  (boto3.client)   :   S3 client object
    """

    S3_client = create_client
    S3_client.create_bucket(Bucket=bucket_name)

    error = (
        "Expecting property name enclosed "
        "in double quotes: line 4 column 1 (char 25)"
    )
    assert str(main(get_bad_json, S3_client)) == error

def test_extract_no_dataset_key(get_no_dataset_key_dcatus_json, create_client):
    """attempt to download a invalid dcatus catalog.
    get_no_dataset_key_dcatus_json (dict)
        :   fixture containing dcatus with no 'dataset' key
    create_client  (boto3.client)   :   S3 client object
    """

    S3_client = create_client
    S3_client.create_bucket(Bucket=bucket_name)

    assert main(get_no_dataset_key_dcatus_json, S3_client) == "invalid dcatus catalog"
   
def test_bad_s3_client( create_bad_client ):
    """attempt to create an S3 client using a bad url endpoint
    create_bad_client (boto3.client)    : S3 client object. 
    """

    S3_client = create_bad_client

    assert str(S3_client) == "Invalid endpoint: garbage"
