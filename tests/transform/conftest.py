import pytest


@pytest.fixture
def transform_route():
    return "http://localhost:3000/translates"


@pytest.fixture
def waf_url():
    return "http://localhost:80"
