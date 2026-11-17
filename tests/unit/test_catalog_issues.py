from unittest.mock import patch

from dcatus_validation.catalog_issues import (
    DUPLICATE_IDENTIFIER_TYPE,
    SPATIAL_TRANSFORMATION_TYPE,
    VALIDATION_ERROR_TYPE,
    CatalogWarning,
    find_catalog_issues,
    summarize_issues,
)
from dcatus_validation.content_warnings import detect_dcat_warnings
from dcatus_validation.messages import open_json
from dcatus_validation.validate import record_validator
from tests.conftest import EXAMPLE_DATA
from tests.conftest import dcatus3_complete_example as _dataset

DCATUS3 = "dcatus3.0 catalog"


def _spatial_warnings(warnings) -> list:
    return [w for w in warnings if w.warning_type == SPATIAL_TRANSFORMATION_TYPE]


UNRESOLVABLE = [{"@type": "Location", "prefLabel": "not a place"}]


def _no_place(name):
    return None


def test_datasets_without_identifier_are_not_duplicates():
    catalog = {"dataset": [_dataset(identifier=None), _dataset(identifier=None)]}

    duplicates, _, _ = find_catalog_issues(catalog, DCATUS3)

    assert duplicates == []


def test_datasets_without_identifier_get_no_warnings():
    catalog = {"dataset": [_dataset(identifier=None, spatial=UNRESOLVABLE)]}

    _, warnings, _ = find_catalog_issues(catalog, DCATUS3, place_lookup=_no_place)

    assert warnings == []


def test_unresolvable_spatial_warns_on_a_valid_dataset():
    catalog = {"dataset": [_dataset(identifier="a", spatial=UNRESOLVABLE)]}

    _, warnings, _ = find_catalog_issues(catalog, DCATUS3, place_lookup=_no_place)

    assert len(_spatial_warnings(warnings)) == 1


def test_unresolvable_spatial_does_not_warn_on_an_invalid_dataset():
    # the harvester only checks spatial while syncing, which skips errored records
    catalog = {"dataset": [_dataset(identifier="a", spatial=UNRESOLVABLE, title=None)]}

    _, warnings, _ = find_catalog_issues(catalog, DCATUS3, place_lookup=_no_place)

    assert _spatial_warnings(warnings) == []


def test_unresolvable_spatial_warns_on_a_valid_dcatus1_1_dataset():
    dataset = open_json(EXAMPLE_DATA / "dcatus" / "dcatus_validator_parity.json")[
        "dataset"
    ][0]
    dataset = {**dataset, "spatial": "not a place"}

    _, warnings, _ = find_catalog_issues(
        {"dataset": [dataset]},
        "dcatus1.1: federal dataset",
        place_lookup=_no_place,
    )

    assert len(_spatial_warnings(warnings)) == 1


def test_issue_types_are_the_harvester_exception_names():
    # dcatus_validation can't import the harvester, so pin the names here
    from harvester.exceptions import (
        DuplicateIdentifierException,
        SpatialTransformationException,
    )

    assert DUPLICATE_IDENTIFIER_TYPE == DuplicateIdentifierException.__name__
    assert SPATIAL_TRANSFORMATION_TYPE == SpatialTransformationException.__name__


def test_summarize_issues_orders_by_severity_then_type():
    warnings = [
        CatalogWarning("a", "dataset", "invalid_tel", "m"),
        CatalogWarning("b", "dataset", SPATIAL_TRANSFORMATION_TYPE, "m"),
        CatalogWarning("c", "dataset", "invalid_tel", "m"),
    ]

    assert summarize_issues(3, 1, warnings) == [
        {"severity": "error", "type": DUPLICATE_IDENTIFIER_TYPE, "count": 1},
        {"severity": "error", "type": VALIDATION_ERROR_TYPE, "count": 3},
        {"severity": "warning", "type": SPATIAL_TRANSFORMATION_TYPE, "count": 1},
        {"severity": "warning", "type": "invalid_tel", "count": 2},
    ]


def test_summarize_issues_omits_zero_counts():
    assert summarize_issues(0, 0, []) == []


def _bad_media_type_distributions(count: int) -> list:
    return [
        {"@type": "Distribution", "mediaType": f"not/a-type-{i}"} for i in range(count)
    ]


def test_one_record_past_the_warning_cap_stops_detection_early():
    catalog = {
        "dataset": [
            _dataset(identifier="a", distribution=_bad_media_type_distributions(50))
        ]
    }

    with patch(
        "dcatus_validation.catalog_issues.detect_dcat_warnings",
        wraps=detect_dcat_warnings,
    ) as detect:
        _, warnings, incomplete = find_catalog_issues(catalog, DCATUS3, max_warnings=5)

    assert len(warnings) == 5
    assert incomplete is True
    assert detect.call_args.args[2] == 5


def test_detection_stops_one_warning_past_the_limit():
    record = _dataset(identifier="a", distribution=_bad_media_type_distributions(50))

    assert len(detect_dcat_warnings(record, limit=3)) == 4


def test_valid_records_skip_python_jsonschema_for_the_spatial_check():
    catalog = {"dataset": [_dataset(identifier="a", spatial=UNRESOLVABLE)]}

    with patch("dcatus_validation.catalog_issues.record_validator") as python:
        _, warnings, _ = find_catalog_issues(catalog, DCATUS3, place_lookup=_no_place)

    python.assert_not_called()
    assert len(_spatial_warnings(warnings)) == 1


def test_records_jsonschema_rs_cant_confirm_still_go_to_python_jsonschema():
    catalog = {"dataset": [_dataset(identifier="a", spatial=UNRESOLVABLE, title=None)]}

    with patch(
        "dcatus_validation.catalog_issues.record_validator", wraps=record_validator
    ) as python:
        _, warnings, _ = find_catalog_issues(catalog, DCATUS3, place_lookup=_no_place)

    python.assert_called_once()
    assert _spatial_warnings(warnings) == []
