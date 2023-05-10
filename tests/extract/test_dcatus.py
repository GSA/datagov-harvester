from datagovharvester.extract.utils import extract_catalog


def test_extract_dcatus(get_dcatus_job, create_client, create_bucket):
    success = False

    url = get_dcatus_job["url"]
    job_id = get_dcatus_job["job_id"]

    S3_client, S3_msg = create_client
    bucket_name, bucket_msg = create_bucket

    final_res, final_error_msg = extract_catalog(url, job_id, S3_client, bucket_name)

    if final_error_msg is None:
        success = True

    assert success
