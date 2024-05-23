import logging.config

from dotenv import load_dotenv

from database.interface import HarvesterDBInterface
from harvester.logger_config import LOGGING_CONFIG

load_dotenv()

logging.config.dictConfig(LOGGING_CONFIG)

db_interface = HarvesterDBInterface()
