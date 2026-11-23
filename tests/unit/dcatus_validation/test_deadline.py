import json
import time
from unittest.mock import patch

import pytest

from dcatus_validation.catalog_issues import _is_schema_valid
from dcatus_validation.content_warnings import detect_dcat_warnings
from dcatus_validation.deadline import (
    ValidationTimedOut,
    check_deadline,
    validation_deadline,
)
from dcatus_validation.spatial import translate_spatial
from tests.conftest import dcatus3_complete_example


def test_without_a_deadline_every_check_passes():
    check_deadline(expected_seconds=10**9)


def test_check_passes_inside_the_budget():
    with validation_deadline(60):
        check_deadline(expected_seconds=1)


def test_check_raises_once_the_budget_is_spent():
    with validation_deadline(0):
        with pytest.raises(ValidationTimedOut):
            check_deadline()


def test_check_raises_for_a_step_expected_to_overrun_the_budget():
    with validation_deadline(60):
        with pytest.raises(ValidationTimedOut):
            check_deadline(expected_seconds=61)


def test_the_budget_ends_with_the_block():
    with validation_deadline(0):
        pass

    check_deadline()


def _polygon_of_about(megabytes: float) -> str:
    point = [-77.12345, 38.12345]
    count = int(megabytes * 1_000_000 / len(json.dumps(point)))
    return json.dumps({"type": "Polygon", "coordinates": [[point] * count]})


def test_a_spatial_value_too_big_for_the_remaining_budget_stops_before_parsing():
    value = _polygon_of_about(5)

    start = time.monotonic()
    with validation_deadline(1):
        with pytest.raises(ValidationTimedOut):
            translate_spatial(value)

    assert time.monotonic() - start < 0.5


def test_spatial_values_are_unaffected_without_a_deadline():
    # the harvest runner sets none
    assert translate_spatial("-77.1,38.1,-76.9,39.2")


def test_warning_detection_stops_once_the_budget_is_spent():
    record = dcatus3_complete_example(identifier="a")

    with validation_deadline(0):
        with pytest.raises(ValidationTimedOut):
            detect_dcat_warnings(record)


def test_an_invalid_record_stops_before_python_jsonschema_once_the_budget_is_spent():
    # jsonschema-rs can't confirm it valid, so it would go to Python jsonschema
    record = dcatus3_complete_example(identifier="a", title=None)

    with patch("dcatus_validation.catalog_issues.record_validator") as python:
        with validation_deadline(0):
            with pytest.raises(ValidationTimedOut):
                _is_schema_valid(record, "dcatus3.0 catalog", "dataset")

    python.assert_not_called()
