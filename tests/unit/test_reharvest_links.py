from harvester.utils.reharvest_links import (
    create_reharvest_token,
    read_reharvest_token,
)


def test_reharvest_token_is_signed_and_recipient_specific():
    secret = "test-secret"
    token = create_reharvest_token(secret, "person@gsa.gov", "source-1", "job-1")
    assert read_reharvest_token(secret, token) == {
        "email": "person@gsa.gov",
        "source_id": "source-1",
        "job_id": "job-1",
    }
    assert read_reharvest_token("other-secret", token) is None
    assert read_reharvest_token(secret, token + "x") is None
    assert token != create_reharvest_token(secret, "other@gsa.gov", "source-1", "job-1")
