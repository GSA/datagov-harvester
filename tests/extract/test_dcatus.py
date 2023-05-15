from datagovharvester.extract.dcatus import extract_json_catalog


def test_extract_dcatus(get_dcatus_job, create_client, create_bucket):
    success = False

    S3_client, S3_msg = create_client
    bucket_name, bucket_msg = create_bucket

    extract_erros = extract_json_catalog(
        get_dcatus_job["url"], S3_client, get_dcatus_job["job_id"], bucket_name
    )

    if len(extract_erros) == 0:
        success = True

    assert success
