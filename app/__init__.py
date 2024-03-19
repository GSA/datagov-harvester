from flask import Flask
from .models import db
from flask_migrate import Migrate
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URI = os.getenv('DATABASE_URI')

def create_app():
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URI
    db.init_app(app)
    
    # Initialize Flask-Migrate
    Migrate(app, db)

    from .routes import register_routes
    register_routes(app)

    return app