import os

from dotenv import load_dotenv
from flask import Flask
from flask_bootstrap import Bootstrap
from flask_migrate import Migrate

from app.scripts.load_manager import load_manager
from database.models import db

load_dotenv()


def create_app():
    app = Flask(__name__, static_url_path="", static_folder="static")

    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URI")
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["SECRET_KEY"] = os.urandom(16)  # TODO: pull from env var
    Bootstrap(app)

    db.init_app(app)

    Migrate(app, db)

    from .routes import register_routes

    register_routes(app)

    with app.app_context():
        db.create_all()
        load_manager()

    return app
