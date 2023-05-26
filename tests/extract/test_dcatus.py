from datagovharvester.extract.dcatus import extract_json_catalog

def test_extract_dcatus(get_dcatus_job, create_client):
    """ download dcat-us json file and store result in s3 bucket.
    get_dcatus_job (dict)   : fixture containing job data (i.e. url, job_id, and source_id ) 
    create_client           : S3 object
    create_bucket           : S3 bucket 
    """

    success = False

    S3_client = create_client
    bucket_name = "test-bucket" 
    
    S3_client.create_bucket(Bucket=bucket_name)

    extraction = extract_json_catalog(
        get_dcatus_job["url"], get_dcatus_job["source_id"], get_dcatus_job["job_id"], create_client, bucket_name
    )

    if len(extraction["errors"]) == 0:
        success = True

    assert success
