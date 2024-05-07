import os

from dotenv import load_dotenv
from flask import Flask
from flask_bootstrap import Bootstrap
from flask_migrate import Migrate

from database.models import db
from app.scripts.load_manager import load_manager

load_dotenv()


def create_app(testing=False):
    app = Flask(__name__, static_url_path="", static_folder="static")

    if testing:
        app.config["TESTING"] = True
        app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///:memory:"
        app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    else:
        app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URI")
        app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
        app.config["SECRET_KEY"] = os.urandom(16)
        Bootstrap(app)

    db.init_app(app)

    if not testing:
        Migrate(app, db)

        from .routes import register_routes

        register_routes(app)

    with app.app_context():
        load_manager()

    return app
