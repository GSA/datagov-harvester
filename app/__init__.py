from flask import Flask
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from .models import db
from flask_migrate import Migrate
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DATABASE_URI = os.getenv("DATABASE_URI")


def smoke_test_job():
    print(f"datetime of execution {str(datetime.now())}")


def create_app():
    app = Flask(__name__)
    app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URI
    app.config["SCHEDULER_JOBSTORES"] = {
        "default": SQLAlchemyJobStore(url=DATABASE_URI.replace("localhost", "db")),
    }
    app.config["SCHEDULER_API_ENABLED"] = True
    app.config["JOBS"] = [
        {
            "id": "smoke-test-job",
            "func": smoke_test_job,
            "trigger": "interval",
            "seconds": 3,
            "replace_existing": True,
        }
    ]
    app.config["SCHEDULER_EXECUTORS"] = {
        "default": {"type": "threadpool", "max_workers": 20}
    }
    app.config["SCHEDULER_JOB_DEFAULTS"] = {"coalesce": False, "max_instances": 3}

    db.init_app(app)

    # Initialize Flask-Migrate
    Migrate(app, db)

    from .routes import register_routes

    register_routes(app)

    return app
