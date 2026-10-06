import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# What the validator app must never pull in: the admin app and harvester
# packages (their imports need a database URI, OIDC settings and CF
# credentials) and the drivers they bring. sqlalchemy/flask_sqlalchemy are not
# listed: APIFlask imports flask_marshmallow, which imports flask_sqlalchemy
# whenever it's installed - a library import with no engine or env behind it.
FORBIDDEN = {
    "app",
    "harvester",
    "database",
    "search",
    "scripts",
    "psycopg",
    "geoalchemy2",
    "opensearchpy",
    "shapely",
}

PROBE = """
import json, sys
from validator_api.wsgi import application
client = application.test_client()
print(json.dumps({
    "modules": sorted({name.split(".")[0] for name in sys.modules}),
    "health": client.get("/health").status_code,
}))
"""


def test_validator_api_imports_with_no_database_or_secrets():
    """
    The validator is public and fetches attacker-chosen URLs, so it must run
    without the admin app's database, secrets or CF credentials. A fresh
    interpreter with an empty environment proves the import chain needs none.
    """
    result = subprocess.run(
        [sys.executable, "-c", PROBE],
        cwd=REPO_ROOT,
        env={"PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert result.returncode == 0, result.stderr
    probe = json.loads(result.stdout.strip().splitlines()[-1])
    assert probe["health"] == 200
    assert not FORBIDDEN & set(probe["modules"])
