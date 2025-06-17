import logging.config
import os

from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from config.logger_config import LOGGING_CONFIG
from database.interface import HarvesterDBInterface

load_dotenv()

logging.config.dictConfig(LOGGING_CONFIG)

DATABASE_URI = os.getenv("DATABASE_URI")

# create a scopedsession for our harvest runner
engine = create_engine(DATABASE_URI)
session_factory = sessionmaker(bind=engine, autoflush=True)
session = scoped_session(session_factory)

db_interface = HarvesterDBInterface(session=session)

SMTP_CONFIG = {
    "server": os.getenv("HARVEST_SMTP_SERVER"),
    "port": 587,
    "use_tls": os.getenv("HARVEST_SMTP_STARTTLS", "true").lower() == "true",
    "username": os.getenv("HARVEST_SMTP_USER"),
    "password": os.getenv("HARVEST_SMTP_PASSWORD"),
    "default_sender": os.getenv("HARVEST_SMTP_SENDER"),
    "base_url": os.getenv("REDIRECT_URI").rsplit("/", 1)[0],
    "recipient": os.getenv("HARVEST_SMTP_RECIPIENT"),
}
