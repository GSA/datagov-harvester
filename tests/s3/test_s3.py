
def test_localstack_is_healthy(get_docker_api_client):

    api_client = get_docker_api_client
    inspect_results = api_client.inspect_container("localstack-container")

    assert inspect_results["State"]["Health"]["Status"] == "healthy"