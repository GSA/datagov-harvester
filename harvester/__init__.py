import logging.config
from .logger_config import LOGGING_CONFIG

# logging data
# TODO: wire this up!
# db_session = get_sqlachemy_session(
#     "postgresql://placeholder-user:placeholder-pass@0.0.0.0:5432/testdb"
# )
db_session = None  # TODO: wire this up!
LOGGING_CONFIG["handlers"]["db"]["session"] = db_session
logging.config.dictConfig(LOGGING_CONFIG)
