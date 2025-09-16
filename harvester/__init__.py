import logging.config
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from config.logger_config import LOGGING_CONFIG
from database.interface import HarvesterDBInterface

from .utils.general_utils import SMTP_CONFIG as SMTP_CONFIG

load_dotenv()

logging.config.dictConfig(LOGGING_CONFIG)

DATABASE_URI = os.getenv("DATABASE_URI")

timeout = os.getenv("DB_TIMEOUT", 300000)  # 5 minutes
# create a scopedsession for our harvest runner
engine = create_engine(
    DATABASE_URI,
    connect_args={
        "options": f"-c statement_timeout={timeout} "
        f"-c idle_in_transaction_session_timeout={timeout}"
    },
)
session_factory = sessionmaker(bind=engine, autoflush=True)
session = scoped_session(session_factory)

db_interface = HarvesterDBInterface(session=session)
