import pytest


@pytest.fixture
def get_dcatus_job():
    """example dcatus job payload"""
    return "http://localhost/dcatus.json"


@pytest.fixture
def get_bad_url():
    """example dcatus job payload with bad url"""
    return "http://localhost/bad_url"


@pytest.fixture
def get_bad_json():
    """example bad json with missing enclosing bracket"""
    return "http://localhost/unclosed.json"


@pytest.fixture
def get_no_dataset_key_dcatus_json():
    """example dcatus json with no 'dataset' key"""
    return "http://localhost/no_dataset_key.json"
