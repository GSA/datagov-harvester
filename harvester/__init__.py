import logging.config
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from config.logger_config import LOGGING_CONFIG
from database.interface import HarvesterDBInterface

from .utils.general_utils import SMTP_CONFIG

load_dotenv()

logging.config.dictConfig(LOGGING_CONFIG)

DATABASE_URI = os.getenv("DATABASE_URI")

# create a scopedsession for our harvest runner
engine = create_engine(DATABASE_URI)
session_factory = sessionmaker(bind=engine, autoflush=True)
session = scoped_session(session_factory)

db_interface = HarvesterDBInterface(session=session)
