import logging.config

from dotenv import load_dotenv

from config.logger_config import LOGGING_CONFIG
from database.interface import HarvesterDBInterface

import os

load_dotenv()

logging.config.dictConfig(LOGGING_CONFIG)

db_interface = HarvesterDBInterface()

SMTP_CONFIG = {
    "server": os.getenv("HARVEST_SMTP_SERVER"),
    "port": 587,
    "use_tls": os.getenv("HARVEST_SMTP_STARTTLS", "true").lower() == "true",
    "username": os.getenv("HARVEST_SMTP_USER"),
    "password": os.getenv("HARVEST_SMTP_PASSWORD"),
    "default_sender": os.getenv("HARVEST_SMTP_SENDER"),
    "base_url": os.getenv("REDIRECT_URI").rsplit("/", 1)[0],
    "recipient": os.getenv("HARVEST_SMTP_RECIPIENT")
}
