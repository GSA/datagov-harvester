import json
import os
import subprocess
from pathlib import Path

import pytest

PROFILE = Path(__file__).resolve().parents[2] / ".profile"


def _profile_environment(runner_environment, next_bound=True):
    """Build the container environment .profile runs in.

    ``next_bound=False`` drops the replacement cluster from VCAP_SERVICES, which is
    the state at rest: no migration in flight, so no `<canonical>-next` instance
    exists to bind.
    """
    environment = {
        **os.environ,
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
                        "credentials": {
                            "host": "opensearch.example",
                            "access_key": "live-access",
                            "secret_key": "live-secret",
                        },
                    },
                    {
                        "name": "datagov-catalog-opensearch-next",
                        "credentials": {
                            "host": "opensearch-next.example",
                            "access_key": "next-access",
                            "secret_key": "next-secret",
                        },
                    },
                ],
            }
        ),
    }
    if not next_bound:
        services = json.loads(environment["VCAP_SERVICES"])
        services["opensearch"] = [
            instance
            for instance in services["opensearch"]
            if not instance["name"].endswith("-next")
        ]
        environment["VCAP_SERVICES"] = json.dumps(services)
    environment.pop("HARVEST_RUNNER_MAX_TASKS", None)
    for name in (
        "OPENSEARCH_SERVICE_NAME",
        "OPENSEARCH_NEXT_SERVICE_NAME",
        "proxy_url",
    ):
        environment.pop(name, None)
    environment.update(runner_environment)
    return environment


def _source_profile(runner_environment, names=(), next_bound=True):
    """Source .profile under ``runner_environment``, reading back ``names``."""
    return subprocess.run(
        [
            "bash",
            "-c",
            'source "$1" >/dev/null; for name in "${@:2}"; do '
            'printf "%s=%s\\n" "$name" "${!name}"; done',
            "bash",
            str(PROFILE),
            *names,
        ],
        capture_output=True,
        env=_profile_environment(runner_environment, next_bound=next_bound),
        text=True,
        timeout=10,
    )


def _profile_variables(runner_environment, names, next_bound=True):
    """Source .profile successfully and return the requested variables."""
    result = _source_profile(runner_environment, names, next_bound=next_bound)
    assert result.returncode == 0, result.stderr
    return dict(
        line.split("=", 1) for line in result.stdout.strip().splitlines() if "=" in line
    )


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
    assert result.stdout.endswith(max_tasks)


def test_profile_defaults_to_the_shared_catalog_opensearch_instance():
    variables = _profile_variables(
        {},
        [
            "OPENSEARCH_SERVICE_NAME",
            "OPENSEARCH_HOST",
            "OPENSEARCH_ACCESS_KEY",
            "OPENSEARCH_SECRET_KEY",
            "OPENSEARCH_NEXT_HOST",
        ],
        next_bound=False,
    )

    assert variables["OPENSEARCH_SERVICE_NAME"] == "datagov-catalog-opensearch"
    assert variables["OPENSEARCH_HOST"] == "opensearch.example"
    assert variables["OPENSEARCH_ACCESS_KEY"] == "live-access"
    assert variables["OPENSEARCH_SECRET_KEY"] == "live-secret"
    # Nothing leaks from the unbound replacement cluster.
    assert variables["OPENSEARCH_NEXT_HOST"] == ""


def test_profile_derives_the_next_cluster_name_from_the_canonical_one():
    """No env var and no restart are needed to reach the replacement cluster.

    The name is derived, so binding the instance is the entire handoff -- which is
    what lets `cf run-task` pick it up immediately in a fresh container.
    """
    variables = _profile_variables(
        {},
        [
            "OPENSEARCH_NEXT_SERVICE_NAME",
            "OPENSEARCH_HOST",
            "OPENSEARCH_NEXT_HOST",
            "OPENSEARCH_NEXT_ACCESS_KEY",
            "OPENSEARCH_NEXT_SECRET_KEY",
        ],
    )

    assert (
        variables["OPENSEARCH_NEXT_SERVICE_NAME"] == "datagov-catalog-opensearch-next"
    )
    # The live cluster is still the live cluster; binding a replacement is inert
    # until the promote renames it.
    assert variables["OPENSEARCH_HOST"] == "opensearch.example"
    assert variables["OPENSEARCH_NEXT_HOST"] == "opensearch-next.example"
    assert variables["OPENSEARCH_NEXT_ACCESS_KEY"] == "next-access"
    assert variables["OPENSEARCH_NEXT_SECRET_KEY"] == "next-secret"


def test_profile_next_name_follows_an_overridden_canonical_name():
    """The derivation is off OPENSEARCH_SERVICE_NAME, not a hardcoded literal, so
    debugging against another instance keeps live and next consistent."""
    variables = _profile_variables(
        {"OPENSEARCH_SERVICE_NAME": "datagov-catalog-opensearch-next"},
        ["OPENSEARCH_NEXT_SERVICE_NAME"],
    )

    assert (
        variables["OPENSEARCH_NEXT_SERVICE_NAME"]
        == "datagov-catalog-opensearch-next-next"
    )


def test_profile_cutover_repoints_live_credentials_at_the_replacement():
    variables = _profile_variables(
        {"OPENSEARCH_SERVICE_NAME": "datagov-catalog-opensearch-next"},
        ["OPENSEARCH_HOST", "OPENSEARCH_ACCESS_KEY", "OPENSEARCH_SECRET_KEY"],
    )

    assert variables["OPENSEARCH_HOST"] == "opensearch-next.example"
    assert variables["OPENSEARCH_ACCESS_KEY"] == "next-access"
    assert variables["OPENSEARCH_SECRET_KEY"] == "next-secret"


@pytest.mark.parametrize(
    ("next_bound", "expected_no_proxy"),
    [
        (False, ".apps.internal,opensearch.example"),
        (True, ".apps.internal,opensearch.example,opensearch-next.example"),
    ],
)
def test_profile_excludes_every_bound_opensearch_host_from_the_proxy(
    next_bound, expected_no_proxy
):
    """Both clusters must bypass the egress proxy, with no stray comma."""
    variables = _profile_variables(
        {"proxy_url": "http://proxy.example:8080"},
        ["no_proxy"],
        next_bound=next_bound,
    )

    assert variables["no_proxy"] == expected_no_proxy


def test_profile_fails_when_the_named_opensearch_instance_is_not_bound():
    """A mistyped service name must stop the app, not start a silent no-op.

    An empty OPENSEARCH_HOST makes the harvest path skip indexing entirely
    without logging an error, so this would otherwise pass the rolling-restart
    health check and go unnoticed until the next compare.
    """
    result = _source_profile({"OPENSEARCH_SERVICE_NAME": "datagov-typo-opensearch"})

    assert result.returncode != 0
    assert "datagov-typo-opensearch" in result.stderr


def test_profile_starts_when_no_next_cluster_is_bound():
    """The at-rest state must boot cleanly.

    Now that the replacement's name is always derived, the lookup for it runs on
    every single start -- including the overwhelmingly common case where no
    migration is in flight and no such instance exists. .profile runs under
    `set -o errexit` and jq exits non-zero when nothing matches, so without the
    `|| true` guards this would fail every boot. That makes this the regression
    test for the whole hardcoding change, not an edge case.
    """
    result = _source_profile(
        {}, ["OPENSEARCH_HOST", "OPENSEARCH_NEXT_HOST"], next_bound=False
    )

    assert result.returncode == 0, result.stderr
    variables = dict(
        line.split("=", 1) for line in result.stdout.strip().splitlines() if "=" in line
    )
    assert variables["OPENSEARCH_HOST"] == "opensearch.example"
    assert variables["OPENSEARCH_NEXT_HOST"] == ""
