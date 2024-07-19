import os

from dotenv import load_dotenv

load_dotenv()

FAKE_VAR = os.getenv("FAKE_VAR")
FOO_VAR = os.getenv("FOO_VAR")
SRC_TITLE = os.getenv("SRC_TITLE")

print(">>>>>")
print(f"clean var {SRC_TITLE}")
print(f"simple var {FOO_VAR}")
print(f"secret {FAKE_VAR}")
print(f"FAKE_VAR equals foo? {FAKE_VAR == 'foo'}")
print(">>>>>")
