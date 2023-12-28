import pytest

from harvester import HarvestSource


@pytest.fixture
def dcat_example():
    """example dcatus harvest source"""
    return HarvestSource(
        url="http://localhost/dcatus/dcatus.json", source_type="dcatus"
    )


@pytest.fixture
def dcat_bad_url():
    """example dcatus harvest source with bad url"""
    return HarvestSource(url="http://localhost/bad_url", source_type="dcatus")


@pytest.fixture
def dcat_bad_json():
    """example bad dcatus json with missing enclosing bracket"""
    return HarvestSource(
        url="http://localhost/dcatus/unclosed.json", source_type="dcatus"
    )


@pytest.fixture
def dcat_no_dataset_key_json():
    """example dcatus json with no 'dataset' key"""
    return HarvestSource(
        url="http://localhost/dcatus/no_dataset_key.json", source_type="dcatus"
    )


@pytest.fixture
def waf_example():
    """example waf"""
    return HarvestSource(url="http://localhost", source_type="waf")
