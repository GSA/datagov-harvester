
"""OpenAPI spec tests."""

class TestOpenAPI:

    def test_openapi_json_endpoint(self, client):
        response = client.get("/openapi.json")
        assert "Datagov Harvester" in response.text
        assert "application/json" == response.content_type

        spec = response.json
        assert "components" in spec
        assert "info" in spec
        assert "paths" in spec
        assert "servers" in spec
        assert "tags" in spec

        assert spec["info"]["title"] == "Datagov Harvester"
