import ast
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
    "validation": client.post("/api/v1/validate", json={
        "schema": "dcatus1.1: federal dataset",
        "fetch_method": "paste",
        "json_text": json.dumps({"dataset": []}),
    }).status_code,
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
    assert probe["validation"] == 200
    assert not FORBIDDEN & set(probe["modules"])


def test_validator_source_has_no_direct_forbidden_imports():
    violations = []
    for package_name in ("validator_api", "dcatus_validation"):
        for path in (REPO_ROOT / package_name).rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.Import):
                    modules = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module:
                    modules = [node.module]
                else:
                    continue

                for module in modules:
                    if module.split(".")[0] in FORBIDDEN:
                        violations.append(
                            f"{path.relative_to(REPO_ROOT)}:{node.lineno}"
                        )

    assert violations == []
