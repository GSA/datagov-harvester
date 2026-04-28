"""OpenAPI spec tests."""

from unittest.mock import patch

from bs4 import BeautifulSoup

from app import create_app


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

    def test_openapi_json_uses_external_route_for_servers(self):
        with patch.dict("os.environ", {"EXTERNAL_ROUTE": "api.example.gov"}):
            app = create_app()
            app.config.update({"TESTING": True})

            with app.test_client() as client:
                response = client.get("/openapi.json")

        assert response.status_code == 200
        assert response.json["servers"] == [{"url": "https://api.example.gov"}]
