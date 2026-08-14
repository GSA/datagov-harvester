"""Route registration: HTML pages on `main`, JSON/OpenAPI routes on `api`."""

from flask import redirect, request

from app.api import api
from app.main import main

# Ordered oldest -> newest; unprefixed `/api` redirects to the LAST entry.
API_VERSIONS = [
    ("v1", api),
]


def register_routes(app):
    app.register_blueprint(main)

    for version, blueprint in API_VERSIONS:
        app.register_blueprint(
            blueprint, name=f"api_{version}", url_prefix=f"/api/{version}"
        )

    latest_version, _ = API_VERSIONS[-1]

    @app.route("/api/<path:subpath>", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    @app.doc(hide=True)
    def api_latest_redirect(subpath):
        target = f"/api/{latest_version}/{subpath}"
        if request.query_string:
            target = f"{target}?{request.query_string.decode()}"
        return redirect(target, code=308)
