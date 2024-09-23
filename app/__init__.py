import os

from dotenv import load_dotenv
from flask import Flask
from flask_bootstrap import Bootstrap
from flask_htmx import HTMX
from flask_migrate import Migrate

from app.filters import usa_icon
from app.scripts.load_manager import load_manager
from database.models import db

load_dotenv()


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
        db.create_all()
        load_manager()

    return app


def add_template_filters(app):
    for fn in [usa_icon]:
        app.add_template_filter(fn)
