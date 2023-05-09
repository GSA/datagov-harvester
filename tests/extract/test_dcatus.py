from datagovharvester.extract.utils import extract_catalog


def test_extract_dcatus(create_s3_client_config, get_dcatus_job):
    success = False

    url = get_dcatus_job["url"]
    job_id = get_dcatus_job["job_id"]

    S3_config = create_s3_client_config

    final_res, final_error_msg = extract_catalog(url, S3_config, job_id)

    print(final_error_msg)
    if final_error_msg is None:
        success = True

    assert success
