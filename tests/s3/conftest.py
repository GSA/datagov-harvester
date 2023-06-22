import pytest
from docker import APIClient


@pytest.fixture
def get_docker_api_client():
    return APIClient()
