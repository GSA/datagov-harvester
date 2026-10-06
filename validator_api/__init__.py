"""The public DCAT-US validation API, deployed as its own Cloud Foundry app
(`datagov-harvest-validator`).

Deliberately minimal: it imports only `dcatus_validation`, APIFlask and
marshmallow - never the admin app, `harvester` or `database` - so it runs with
no database, secrets or CF credentials (tests/unit/test_validator_import_clean.py).
datagov-harvest-proxy routes /api/v1/validate here, for API callers and for the
admin app's /validate/ page, which submits from the browser.
"""

import logging
import logging.config
import os
from urllib.parse import urlsplit

from apiflask import APIFlask
from flask import jsonify, redirect, request
from werkzeug.exceptions import RequestEntityTooLarge

from dcatus_validation.limits import MAX_REQUEST_BYTES, MAX_UPLOAD_MB

logger = logging.getLogger("validator_api")

HSTS_MAX_AGE_SECONDS = 60 * 60 * 24 * 365
HSTS_HEADER = f"max-age={HSTS_MAX_AGE_SECONDS}; includeSubDomains; preload"

# Served under /validator/ so datagov-harvest-proxy can expose them on the
# harvester's own domain without colliding with the admin app's /openapi/docs.
DOCS_PATH = "/validator/docs"
SPEC_PATH = "/validator/openapi.json"

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": (
                "[%(asctime)s] %(levelname)s "
                "[%(name)s.%(funcName)s:%(lineno)d] %(message)s"
            )
        },
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        name: {"handlers": ["console"], "level": "INFO", "propagate": False}
        for name in ("validator_api", "dcatus_validation")
    },
}


def _external_route_to_server_url(route: str | None) -> str | None:
    """Return a normalized external server URL, or None for empty input."""
    if not route:
        return None

    route = route.strip().rstrip("/")
    if not route:
        return None

    if urlsplit(route).scheme:
        return route

    return f"https://{route}"


def create_app():
    logging.config.dictConfig(LOGGING_CONFIG)

    app = APIFlask(
        __name__,
        title="Datagov Validator",
        version="0.1.0",
        docs_path=DOCS_PATH,
        spec_path=SPEC_PATH,
    )

    # The harvester's external route, so the docs' "Try it out" goes through
    # the proxy like every other caller.
    external_server_url = _external_route_to_server_url(os.getenv("EXTERNAL_ROUTE"))
    if external_server_url:
        app.config["SERVERS"] = [{"url": external_server_url}]

    app.config["MAX_CONTENT_LENGTH"] = MAX_REQUEST_BYTES
    # Lets synthetic monitoring confirm a request forwarded by
    # datagov-harvest-proxy actually reached this app.
    app.config["SERVED_BY"] = os.getenv("SERVED_BY", "datagov-harvest-validator")

    @app.after_request
    def apply_headers(response):
        response.headers["X-Served-By"] = app.config["SERVED_BY"]
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Strict-Transport-Security"] = HSTS_HEADER
        # Every validation answer is specific to the submitted document.
        if request.method not in {"GET", "HEAD"} or response.status_code >= 400:
            response.headers["Cache-Control"] = "private, no-store, max-age=0"
        return response

    @app.errorhandler(RequestEntityTooLarge)
    def handle_request_entity_too_large(error):
        """
        Same {"error": ...} shape as the route's own refusals, rather than
        APIFlask's generic {"message": ...} one.
        """
        logger.warning(
            "Rejected request over the %sMB limit path=%s", MAX_UPLOAD_MB, request.path
        )
        message = f"Submission too large - must be {MAX_UPLOAD_MB}MB or less."
        return jsonify({"error": message}), 413

    from validator_api.api import api

    app.register_blueprint(api, name="api_v1", url_prefix="/api/v1")

    @app.route(
        "/api/<path:subpath>",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
        merge_slashes=False,
    )
    @app.doc(hide=True)
    def api_latest_redirect(subpath):
        """Unversioned /api/... (e.g. /api/validate) goes to the latest version."""
        if subpath.split("/", 1)[0] == "v1":
            return jsonify({"message": "Not Found"}), 404

        target = f"/api/v1/{subpath}"
        if request.query_string:
            target = f"{target}?{request.query_string.decode()}"

        # Browsers treat backslashes like path separators. Normalize them, then
        # verify the redirect remains relative even for an adversarial route.
        target = target.replace("\\", "/")
        parsed_target = urlsplit(target)
        if parsed_target.scheme or parsed_target.netloc:
            return jsonify({"message": "Not Found"}), 404
        return redirect(target, code=308)

    @app.get("/health")
    @app.doc(hide=True)
    def health():
        """Cloud Foundry's http health check (manifest.yml)."""
        return jsonify({"status": "ok"})

    @app.get("/")
    @app.doc(hide=True)
    def index():
        return redirect(DOCS_PATH)

    return app
