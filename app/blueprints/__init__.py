from . import deps
from .api import api
from .main import main


def register_routes(app):
    app.register_blueprint(main)
    app.register_blueprint(api)
