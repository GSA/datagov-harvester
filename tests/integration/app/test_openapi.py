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

    def test_openapi_tags_group_by_resource(self, client):
        response = client.get("/openapi.json")
        spec = response.json

        # the unversioned `/api` alias is a hidden redirect (see
        # app/routes.py), so every documented path belongs to a real,
        # pinned version -- no separate "latest" tag/paths to duplicate.
        assert spec["tags"] == [
            {"name": "Harvest Jobs"},
            {"name": "Harvest Records"},
            {"name": "Harvest Sources"},
            {"name": "Organizations"},
            {"name": "Validate"},
        ]
        assert all(path.startswith("/api/v1/") for path in spec["paths"])

        expected_tags = {
            "/api/v1/harvest_error/{error_id}": "Harvest Records",
            "/api/v1/harvest_job/{job_id}": "Harvest Jobs",
            "/api/v1/harvest_job/{job_id}/errors/{error_type}": "Harvest Jobs",
            "/api/v1/harvest_job_errors/": "Harvest Jobs",
            "/api/v1/harvest_jobs/": "Harvest Jobs",
            "/api/v1/harvest_record/{record_id}": "Harvest Records",
            "/api/v1/harvest_record/{record_id}/errors": "Harvest Records",
            "/api/v1/harvest_record/{record_id}/raw": "Harvest Records",
            "/api/v1/harvest_record/{record_id}/transformed": "Harvest Records",
            "/api/v1/harvest_record_errors/": "Harvest Records",
            "/api/v1/harvest_records/": "Harvest Records",
            "/api/v1/harvest_sources/": "Harvest Sources",
            "/api/v1/organization/{org_identifier}": "Organizations",
            "/api/v1/organization_list/": "Organizations",
            "/api/v1/organizations/": "Organizations",
            "/api/v1/validate": "Validate",
        }
        assert set(expected_tags) == set(spec["paths"])
        for path, operations in spec["paths"].items():
            for operation in operations.values():
                assert operation["tags"] == [expected_tags[path]]

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
