import logging
import os

from apiflask import APIFlask
from dotenv import load_dotenv
from flask import Flask
from flask_bootstrap import Bootstrap
from flask_htmx import HTMX
from flask_migrate import Migrate
from flask_talisman import Talisman

from app.filters import else_na, usa_icon, utc_isoformat
from database.models import db
from harvester.lib.load_manager import LoadManager
from scripts.sync_datasets import register_cli

logger = logging.getLogger("harvest_admin")

load_manager = LoadManager()

load_dotenv()

# fixes a bug with Flask-HTMX not being able to find the app context
htmx = None


def create_app():
    app = APIFlask(__name__, static_url_path="", static_folder="static", docs_path=None)

    # OpenAPI fields
    app.config["INFO"] = {
        "title": "Datagov Harvester",
        "version": "0.1.0",
    }

    # don't include auth information in the OpenAPI spec
    @app.spec_processor
    def remove_auth(spec):
        del spec["components"]["securitySchemes"]
        return spec

    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URI")
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SECRET_KEY"] = os.getenv("FLASK_APP_SECRET_KEY")
    global htmx
    htmx = HTMX(app)

    db.init_app(app)

    Migrate(app, db)

    from .routes import register_routes

    register_routes(app)

    from .commands import register_commands

    register_commands(app)

    add_template_filters(app)
    register_cli(app)

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

    # Content-Security-Policy headers
    # single quotes need to appear in some of the strings
    csp = {
        "default-src": "'self'",
        "script-src": " ".join([
            "'self'", "'unsafe-hashes'",
            "https://www.googletagmanager.com",
        ]),
        "font-src": " ".join([
            "'self'",  # USWDS fonts
            "https://cdnjs.cloudflare.com",  # font awesome
        ]),
        "img-src": " ".join([
            "'self'", "data:",
            "https://s3-us-gov-west-1.amazonaws.com",  # GSA Starmark
            "https://raw.githubusercontent.com",  # github logos repo
        ]),
        "connect-src": " ".join([
            "'self'",
        ]),
        "frame-src": "https://www.googletagmanager.com",
        "style-src-attr": " ".join([
            "'self'",
        ]),
        "style-src-elem": " ".join([
            "'self'", "'unsafe-hashes'",  # local styles.css
            "https://cdnjs.cloudflare.com",  # font-awesome
            "'sha256-faU7yAF8NxuMTNEwVmBz+VcYeIoBQ2EMHW3WaVxCvnk='",  # htmx.min.js
        ]),
    }
    Talisman(app, content_security_policy=csp,
             content_security_policy_nonce_in=['script-src', 'style-src-elem'],
             # our https connections are terminated outside this app
             force_https=False)


    return app


def add_template_filters(app):
    for fn in [usa_icon, else_na, utc_isoformat]:
        app.add_template_filter(fn)
