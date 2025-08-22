import logging
import os

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)

session = requests.Session()

username = os.getenv("DATAGOV_BASIC_AUTH_USER")
password = os.getenv("DATAGOV_BASIC_AUTH_PASS")

if all([username, password]):
    session.auth = HTTPBasicAuth(username, password)
else:
    logger.warning("Basic auth credentials not set")
