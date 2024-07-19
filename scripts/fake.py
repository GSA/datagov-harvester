import os

from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("CKAN_API_TOKEN")
FOO_VAR = os.getenv("FOO_VAR")
SRC_TITLE = os.getenv("SRC_TITLE")

print(">>>>>")
print(f"clean var {SRC_TITLE}")
print(f"simple var {FOO_VAR}")
print(f"secret {API_TOKEN}")
print(f"secret is None? {API_TOKEN is None}")
print(">>>>>")
