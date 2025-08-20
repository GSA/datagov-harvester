import logging
import os

from dotenv import load_dotenv
from flask import Flask
from flask_bootstrap import Bootstrap
from flask_htmx import HTMX
from flask_migrate import Migrate

from app.filters import else_na, usa_icon, utc_isoformat
from database.models import db
from harvester.lib.load_manager import LoadManager

logger = logging.getLogger("harvest_admin")

load_manager = LoadManager()

load_dotenv()

# fixes a bug with Flask-HTMX not being able to find the app context
htmx = None


def create_app():
    app = Flask(__name__, static_url_path="", static_folder="static")

    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URI")
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SECRET_KEY"] = os.getenv("FLASK_APP_SECRET_KEY")
    Bootstrap(app)
    global htmx
    htmx = HTMX(app)

    db.init_app(app)

    Migrate(app, db)

    from .routes import register_routes

    register_routes(app)

    add_template_filters(app)

    with app.app_context():
        # SQL-Alchemy can't be used to create the schema here
        # Instead, `flask db upgrade` must already have been run
        # db.create_all()
        try:
            load_manager.start()
        except Exception as e:
            # we need to get to app start up, so ignore all errors
            # from the load manager but log them
            logger.warning("Load manager startup failed with exception: %s", repr(e))

    return app


def add_template_filters(app):
    for fn in [usa_icon, else_na, utc_isoformat]:
        app.add_template_filter(fn)
