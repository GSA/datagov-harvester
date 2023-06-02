from harvester import bucket_name
from harvester.extract import main


def test_extract_dcatus(get_dcatus_job, create_client):
    """download dcat-us json file and store result in s3 bucket.
    get_dcatus_job (dict)           :   fixture containing job data
    create_client  (boto3.client)   :   S3 client object
    """

    S3_client = create_client
    S3_client.create_bucket(Bucket=bucket_name)

    assert main(get_dcatus_job, S3_client)


def test_extract_bad_url(get_bad_url, create_client):
    """attempt to download a bad url.
    get_bad_url (dict)              :   fixture containing job data with bad url
    create_client  (boto3.client)   :   S3 client object
    """

    S3_client = create_client
    S3_client.create_bucket(Bucket=bucket_name)

    if str(main(get_bad_url, S3_client)) == "non-200 status code":
        assert True
    else:
        assert False


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
    if str(main(get_bad_json, S3_client)) == error:
        assert True
    else: 
        assert False


def test_extract_no_dataset_key(get_no_dataset_key_dcatus_json, create_client):
    """attempt to download a invalid dcatus catalog.
    get_no_dataset_key_dcatus_json (dict)
        :   fixture containing dcatus with no 'dataset' key
    create_client  (boto3.client)   :   S3 client object
    """

    S3_client = create_client
    S3_client.create_bucket(Bucket=bucket_name)

    if main(get_no_dataset_key_dcatus_json, S3_client) == "invalid dcatus catalog":
        assert True
    else:
        assert False
   
def test_bad_s3_client( create_bad_client ):

    S3_client = create_bad_client

    if str(S3_client) == "Invalid endpoint: garbage":
        assert True
    else:
        assert False
