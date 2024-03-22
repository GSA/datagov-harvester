import os

from dotenv import load_dotenv
from flask import Flask
from flask_migrate import Migrate

from .models import db

load_dotenv()

DATABASE_URI = os.getenv("DATABASE_URI")


def create_app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URI
    db.init_app(app)

    # Initialize Flask-Migrate
    Migrate(app, db)

    from .routes import register_routes

    register_routes(app)

    return app
