"""Route registration: HTML pages on `main`, JSON/OpenAPI routes on `api`."""

from app.api import api
from app.main import main


def register_routes(app):
    app.register_blueprint(main)
    app.register_blueprint(api)
