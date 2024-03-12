import logging.config
from harvester.logger_config import LOGGING_CONFIG
from dotenv import load_dotenv

load_dotenv()

logging.config.dictConfig(LOGGING_CONFIG)
