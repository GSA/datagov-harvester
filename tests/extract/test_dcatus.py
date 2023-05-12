from datagovharvester.extract.dcatus import extract_dcatus_catalog


def test_extract_dcatus(get_dcatus_job, create_client, create_bucket):
    success = False

    S3_client, S3_msg = create_client
    bucket_name, bucket_msg = create_bucket

    final_res, final_error_msg = extract_dcatus_catalog(
        get_dcatus_job["url"], S3_client, get_dcatus_job["job_id"], bucket_name
    )

    if final_error_msg is None:
        success = True

    assert success
