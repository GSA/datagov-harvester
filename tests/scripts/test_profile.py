import json
import os
import subprocess
from pathlib import Path

import pytest

PROFILE = Path(__file__).resolve().parents[2] / ".profile"


def _profile_environment(runner_environment):
    environment = {
        **os.environ,
        "CF_INSTANCE_GUID": "instance-guid",
        "VCAP_APPLICATION": json.dumps({"application_name": "datagov-harvest"}),
        "VCAP_SERVICES": json.dumps(
            {
                "user-provided": [
                    {
                        "name": "datagov-harvest-secrets",
                        "credentials": {},
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
    environment.pop("HARVEST_RUNNER_MAX_TASKS", None)
    environment.update(runner_environment)
    return environment


@pytest.mark.parametrize(
    ("runner_environment", "max_tasks"),
    [
        ({}, "3"),
        ({"HARVEST_RUNNER_MAX_TASKS": "0"}, "0"),
        ({"HARVEST_RUNNER_MAX_TASKS": "3"}, "3"),
    ],
)
def test_profile_loads_max_tasks_from_environment_with_default(
    runner_environment, max_tasks
):
    result = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; printf "%s" "$HARVEST_RUNNER_MAX_TASKS"',
            "bash",
            str(PROFILE),
        ],
        capture_output=True,
        env=_profile_environment(runner_environment),
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert (
        f"Harvester startup: HARVEST_RUNNER_MAX_TASKS={max_tasks} "
        "CF_INSTANCE_GUID=instance-guid"
    ) in result.stdout
    assert result.stdout.endswith(max_tasks)
