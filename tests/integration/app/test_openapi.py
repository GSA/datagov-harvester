"""OpenAPI spec tests."""

from bs4 import BeautifulSoup


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

    def test_openapi_swagger(self, client):
        response = client.get("/openapi/docs")
        assert "OpenAPI Documentation" in response.text
        soup = BeautifulSoup(response.text, "html.parser")
        assert soup.find("div", id="swagger-ui") is not None
        assert any(
            "swagger-ui-bundle.js" in script_el.get("src", "")
            for script_el in soup.find_all("script")
        )
