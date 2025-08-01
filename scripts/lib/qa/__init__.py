import os

import requests
from requests.auth import HTTPBasicAuth

session = requests.Session()

username = os.getenv("DATAGOV_BASIC_AUTH_USER")
password = os.getenv("DATAGOV_BASIC_AUTH_PASS")

session.auth = HTTPBasicAuth(username, password)
