from apiflask import APIBlueprint, APIFlask
from flask import Blueprint

from app import routes as routes_module


def test_api_alias_redirects_to_last_registered_version(monkeypatch):
    """`/api` (unversioned) must redirect to whichever version is LAST in
    API_VERSIONS, so shipping a new version is just appending to that list
    -- no separate step to move the alias. See GSA/data.gov#6236."""
    v1 = APIBlueprint("test_api_v1", __name__)

    @v1.get("/ping")
    def ping_v1():
        return "v1"

    v2 = APIBlueprint("test_api_v2", __name__)

    @v2.get("/ping")
    def ping_v2():
        return "v2"

    monkeypatch.setattr(routes_module, "main", Blueprint("main", __name__))
    monkeypatch.setattr(routes_module, "API_VERSIONS", [("v1", v1), ("v2", v2)])

    app = APIFlask(__name__)
    routes_module.register_routes(app)

    with app.test_client() as client:
        redirect = client.get("/api/ping")
        assert redirect.status_code == 308
        assert redirect.location == "/api/v2/ping"

        assert client.get("/api/v1/ping").get_data(as_text=True) == "v1"
        assert client.get("/api/v2/ping").get_data(as_text=True) == "v2"
