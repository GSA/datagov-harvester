import json
import os
import subprocess
from pathlib import Path

import pytest

PROFILE = Path(__file__).resolve().parents[2] / ".profile"


def _profile_environment(secret_credentials):
    return {
        **os.environ,
        "CF_INSTANCE_GUID": "instance-guid",
        "VCAP_APPLICATION": json.dumps({"application_name": "datagov-harvest"}),
        "VCAP_SERVICES": json.dumps(
            {
                "user-provided": [
                    {
                        "name": "datagov-harvest-secrets",
                        "credentials": secret_credentials,
                    }
                ],
                "database": [
                    {
                        "name": "datagov-harvest-db",
                        "credentials": {"uri": "postgres://example"},
                    }
                ],
                "smtp": [
                    {
                        "name": "datagov-harvest-smtp",
                        "credentials": {
                            "domain_arn": (
                                "arn:aws:ses:us-east-1:123456789012:"
                                "identity/ses-example.appmail.cloud.gov"
                            )
                        },
                    }
                ],
                "opensearch": [
                    {
                        "name": "datagov-catalog-opensearch",
                        "credentials": {"host": "opensearch.example"},
                    }
                ],
            }
        ),
    }


@pytest.mark.parametrize(
    ("secret_credentials", "max_tasks", "enabled", "effective_max_tasks"),
    [
        ({}, "3", "true", "3"),
        ({"HARVEST_RUNNER_MAX_TASKS": "5"}, "5", "true", "5"),
        (
            {
                "HARVEST_RUNNER_MAX_TASKS": "5",
                "HARVEST_RUNNER_ENABLED": "false",
            },
            "5",
            "false",
            "0",
        ),
    ],
)
def test_profile_loads_runner_settings_from_secrets_with_defaults(
    secret_credentials, max_tasks, enabled, effective_max_tasks
):
    result = subprocess.run(
        [
            "bash",
            "-c",
            (
                'source "$1"; printf "%s:%s" '
                '"$HARVEST_RUNNER_MAX_TASKS" "$HARVEST_RUNNER_ENABLED"'
            ),
            "bash",
            str(PROFILE),
        ],
        capture_output=True,
        env=_profile_environment(secret_credentials),
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert (
        f"Harvester startup: HARVEST_RUNNER_MAX_TASKS={max_tasks} "
        f"HARVEST_RUNNER_ENABLED={enabled} "
        f"EFFECTIVE_HARVEST_RUNNER_MAX_TASKS={effective_max_tasks} "
        "CF_INSTANCE_GUID=instance-guid"
    ) in result.stdout
    assert result.stdout.endswith(f"{max_tasks}:{enabled}")
