import pytest

from harvester import Source


@pytest.fixture
def dcat_example():
    """example dcatus harvest source"""
    return Source(url="http://localhost/dcatus/dcatus.json", source_type="dcatus")


@pytest.fixture
def dcat_bad_url():
    """example dcatus harvest source with bad url"""
    return Source(url="http://localhost/bad_url", source_type="dcatus")


@pytest.fixture
def dcat_bad_json():
    """example bad dcatus json with missing enclosing bracket"""
    return Source(url="http://localhost/dcatus/unclosed.json", source_type="dcatus")


@pytest.fixture
def dcat_no_dataset_key_json():
    """example dcatus json with no 'dataset' key"""
    return Source(
        url="http://localhost/dcatus/no_dataset_key.json", source_type="dcatus"
    )


@pytest.fixture
def waf_example():
    """example waf"""
    return Source(url="http://localhost", source_type="waf")
