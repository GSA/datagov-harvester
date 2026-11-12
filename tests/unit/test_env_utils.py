import json

import pytest

from harvester.utils.env_utils import with_environment_prefix


@pytest.mark.parametrize(
    "space_name,expected",
    [
        ("development", "Dev Harvest Job Failed"),
        ("staging", "Stage Harvest Job Failed"),
        ("prod", "Harvest Job Failed"),
    ],
)
def test_with_environment_prefix_by_space(monkeypatch, space_name, expected):
    monkeypatch.setenv("VCAP_APPLICATION", json.dumps({"space_name": space_name}))
    assert with_environment_prefix("Harvest Job Failed") == expected


@pytest.mark.parametrize("vcap", [None, "not json", "[]", "{}"])
def test_with_environment_prefix_without_space(monkeypatch, vcap):
    if vcap is None:
        monkeypatch.delenv("VCAP_APPLICATION", raising=False)
    else:
        monkeypatch.setenv("VCAP_APPLICATION", vcap)
    assert with_environment_prefix("Harvest Job Completed") == "Harvest Job Completed"
