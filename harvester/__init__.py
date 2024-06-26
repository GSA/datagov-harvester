import logging.config

from dotenv import load_dotenv

from config.logger_config import LOGGING_CONFIG
from database.interface import HarvesterDBInterface

load_dotenv()

logging.config.dictConfig(LOGGING_CONFIG)

db_interface = HarvesterDBInterface()
