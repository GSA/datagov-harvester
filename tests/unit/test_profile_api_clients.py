"""Run the real `.profile` extraction of HARVEST_API_CLIENTS against synthetic
VCAP_SERVICES fixtures, under the same errexit/pipefail settings `.profile`
uses, and feed the result into the Python parser."""

import hashlib
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from app.api_credentials import parse_api_clients
from app.startup_validation import StartupValidationError

PROFILE = Path(__file__).parents[2] / ".profile"
DIGEST = "sha256:" + hashlib.sha256(b"synthetic-profile-key").hexdigest()
OTHER_DIGEST = "sha256:" + hashlib.sha256(b"synthetic-other-key").hexdigest()
UNSET = object()

pytestmark = pytest.mark.skipif(
    not (shutil.which("bash") and shutil.which("jq")),
    reason="needs bash and jq, as on Cloud Foundry",
)


def _profile_snippet() -> str:
    text = PROFILE.read_text()
    function = re.search(
        r"^function vcap_get_service \(\) \{\n.*?^\}\n", text, re.M | re.S
    )
    assert function, "vcap_get_service not found in .profile"
    lines = [
        line
        for line in text.splitlines()
        if "HARVEST_API_CLIENTS" in line and not line.lstrip().startswith("#")
    ]
    # Guard against the export masking a failed substitution again.
    assert lines == [lines[0], "export HARVEST_API_CLIENTS"], lines
    assert lines[0].startswith("HARVEST_API_CLIENTS=$(")
    return function.group(0) + "\n".join(lines) + "\n"


def run_profile(credential=UNSET, *, bind_service=True):
    credentials = {"HARVEST_API_TOKEN": "synthetic-legacy"}
    if credential is not UNSET:
        credentials["HARVEST_API_CLIENTS"] = credential
    services = {"user-provided": []}
    if bind_service:
        services["user-provided"].append(
            {"name": "datagov-harvest-secrets", "credentials": credentials}
        )
    script = (
        "set -o errexit\nset -o pipefail\n"
        + _profile_snippet()
        + 'printf "%s" "${HARVEST_API_CLIENTS-<unset>}"\n'
        + "echo REACHED_END >&2\n"
    )
    return subprocess.run(
        ["bash", "-c", script],
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin",
            "HARVEST_SERVICE_NAME": "datagov-harvest",
            "VCAP_SERVICES": json.dumps(services),
        },
        capture_output=True,
        text=True,
        timeout=30,
    )


@pytest.mark.parametrize(
    "kwargs",
    [{}, {"credential": None}, {"credential": ""}, {"bind_service": False}],
    ids=["absent", "json-null", "empty-string", "service-not-bound"],
)
def test_missing_values_export_empty_and_mean_no_clients(kwargs):
    result = run_profile(**kwargs)
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert dict(parse_api_clients(result.stdout)) == {}


def test_json_encoded_string_is_passed_through_verbatim():
    raw = json.dumps({"client-a": DIGEST, "client-b": [OTHER_DIGEST]})
    result = run_profile(raw)
    assert result.returncode == 0, result.stderr
    assert result.stdout == raw
    assert sorted(parse_api_clients(result.stdout).values()) == [
        "client-a",
        "client-b",
    ]


def test_duplicate_client_ids_survive_extraction_and_are_rejected():
    raw = f'{{"client-a": "{DIGEST}", "client-a": "{OTHER_DIGEST}"}}'
    result = run_profile(raw)
    assert result.returncode == 0, result.stderr
    assert result.stdout == raw
    with pytest.raises(StartupValidationError, match="more than once"):
        parse_api_clients(result.stdout)


def test_malformed_string_reaches_python_and_fails_startup():
    result = run_profile("{not json")
    assert result.returncode == 0, result.stderr
    with pytest.raises(StartupValidationError, match="not valid JSON"):
        parse_api_clients(result.stdout)


@pytest.mark.parametrize(
    "credential",
    [{"client-a": DIGEST}, [DIGEST], 42, True],
    ids=["native-object", "array", "number", "boolean"],
)
def test_non_string_json_aborts_the_profile_without_echoing_it(credential):
    result = run_profile(credential)
    assert result.returncode != 0
    assert "REACHED_END" not in result.stderr  # errexit stopped .profile
    assert "must be a JSON-encoded string" in result.stderr
    assert DIGEST[7:] not in result.stderr + result.stdout


# Whitespace runs, tabs, glob characters, expansions and quotes that unquoted
# `echo $VCAP_SERVICES` would collapse, expand or split.
TRICKY_VALUE = "a  b\t\tc  *  ?  [ab]  $HOME  `id`  $(id)  \\n  'q'  {1,2}  ~ "


def run_vcap_get_service(credentials, path, cwd):
    function = re.search(
        r"^function vcap_get_service \(\) \{\n.*?^\}\n",
        PROFILE.read_text(),
        re.M | re.S,
    ).group(0)
    script = (
        "set -o errexit\nset -o pipefail\n"
        + function
        + f'value=$(vcap_get_service secrets "{path}")\n'
        + 'printf "%s" "$value"\n'
    )
    services = {
        "user-provided": [
            {"name": "datagov-harvest-secrets", "credentials": credentials}
        ]
    }
    return subprocess.run(
        ["bash", "-c", script],
        cwd=cwd,
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin",
            "HOME": "/nonexistent-home",
            "HARVEST_SERVICE_NAME": "datagov-harvest",
            "VCAP_SERVICES": json.dumps(services),
        },
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_vcap_get_service_preserves_whitespace_and_metacharacters(tmp_path):
    # Files a glob in the value would match if VCAP_SERVICES were unquoted.
    for name in ("a", "b", "zzz-glob-target"):
        (tmp_path / name).write_text("")
    result = run_vcap_get_service(
        {"SYNTHETIC_SECRET": TRICKY_VALUE}, ".credentials.SYNTHETIC_SECRET", tmp_path
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == TRICKY_VALUE
    assert "zzz-glob-target" not in result.stdout


def test_api_clients_string_keeps_its_exact_spacing(tmp_path):
    (tmp_path / "zzz-glob-target").write_text("")
    raw = '{"client-a":   "' + DIGEST + '",\t"client-b": [ "' + OTHER_DIGEST + '" ]}'
    result = run_vcap_get_service(
        {"HARVEST_API_CLIENTS": raw}, ".credentials.HARVEST_API_CLIENTS", tmp_path
    )
    assert result.stdout == raw
    assert sorted(parse_api_clients(result.stdout).values()) == [
        "client-a",
        "client-b",
    ]


@pytest.mark.parametrize(
    "credential",
    [
        '{"client-a": "' + DIGEST + '"}\x00',
        '\x00{"client-a": "' + DIGEST + '"}',
        '{"client-a"\x00: "' + DIGEST + '"}',
        "\x00",
    ],
    ids=["trailing", "leading", "embedded", "only-nul"],
)
def test_nul_character_aborts_instead_of_being_dropped(credential):
    # Bash command substitution drops NUL bytes with only a warning, which
    # would turn this malformed value into a valid-looking one.
    result = run_profile(credential)
    assert result.returncode != 0
    assert "REACHED_END" not in result.stderr
    assert "contains a NUL character" in result.stderr
    assert DIGEST[7:] not in result.stderr + result.stdout
