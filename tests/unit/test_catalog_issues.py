import copy

from dcatus_validation.catalog_issues import (
    SPATIAL_TRANSFORMATION_TYPE,
    find_catalog_issues,
)
from dcatus_validation.messages import open_json
from dcatus_validation.schema_paths import DCATUS3_COMPLETE_EXAMPLE

DCATUS3 = "dcatus3.0 catalog"
BASE_DATASET = open_json(DCATUS3_COMPLETE_EXAMPLE)


def _dataset(**changes) -> dict:
    record = copy.deepcopy(BASE_DATASET)
    record.update(changes)
    return {k: v for k, v in record.items() if v is not None}


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
    dataset = open_json("example_data/dcatus/dcatus_validator_parity.json")["dataset"][
        0
    ]
    dataset = {**dataset, "spatial": "not a place"}

    _, warnings, _ = find_catalog_issues(
        {"dataset": [dataset]},
        "dcatus1.1: federal dataset",
        place_lookup=_no_place,
    )

    assert len(_spatial_warnings(warnings)) == 1
