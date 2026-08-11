"""Route registration: HTML pages on `main`, JSON/OpenAPI routes on `api`."""

from app.api import api
from app.main import main


def register_routes(app):
    app.register_blueprint(main)
    app.register_blueprint(api)
    # `/api/v1` is the versioned home for this API; the unprefixed `/api`
    # paths stay aliased to the same routes for existing consumers.
    # See GSA/data.gov#5128.
    app.register_blueprint(api, name="api_v1", url_prefix="/api/v1")
