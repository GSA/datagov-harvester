import pytest
from pathlib import Path
from datagovharvester.utils.json_utilities import open_json

BASE_DIR = Path(__file__).parents[3]
DATA_DIR = BASE_DIR / "data" / "dcatus"
SCHEMA_DIR = DATA_DIR / "schemas"
JSON_DIR = DATA_DIR / "jsons"


@pytest.fixture
def open_dataset_schema():
    dataset_schema = SCHEMA_DIR / "dataset.json"
    return open_json(dataset_schema)[0]


@pytest.fixture
def open_catalog_schema():
    catalog_schema = SCHEMA_DIR / "catalog.json"
    return open_json(catalog_schema)[0]


# invalid
@pytest.fixture
def open_numerical_title_json():
    json_file = JSON_DIR / "numerical-title.data.json"
    return open_json(json_file)[0]


# valid
@pytest.fixture
def open_collection_1_parent_2_children_json():
    json_file = JSON_DIR / "collection-1-parent-2-children.data.json"
    return open_json(json_file)[0]


# invalid
@pytest.fixture
def open_missing_catalog_json():
    json_file = JSON_DIR / "missing-catalog.data.json"
    return open_json(json_file)[0]


# invalid
@pytest.fixture
def open_ny_json():
    json_file = JSON_DIR / "ny.data.json"
    return open_json(json_file)[0]


# invalid
@pytest.fixture
def open_missing_identifier_title_json():
    json_file = JSON_DIR / "missing-identifier-title.data.json"
    return open_json(json_file)[0]


# invalid
@pytest.fixture
def open_missing_dataset_fields_json():
    json_file = JSON_DIR / "missing-dataset-fields.data.json"
    return open_json(json_file)[0]


# valid
@pytest.fixture
def open_usda_gov_json():
    json_file = JSON_DIR / "usda.gov.data.json"
    return open_json(json_file)[0]


# valid
@pytest.fixture
def open_arm_json():
    json_file = JSON_DIR / "arm.data.json"
    return open_json(json_file)[0]


# valid
@pytest.fixture
def open_large_spatial_json():
    json_file = JSON_DIR / "large-spatial.data.json"
    return open_json(json_file)[0]


# valid
@pytest.fixture
def open_reserved_title_json():
    json_file = JSON_DIR / "reserved-title.data.json"
    return open_json(json_file)[0]


# valid
@pytest.fixture
def open_collection_2_parent_4_children_json():
    json_file = JSON_DIR / "collection-2-parent-4-children.data.json"
    return open_json(json_file)[0]


# valid
@pytest.fixture
def open_geospatial_json():
    json_file = JSON_DIR / "geospatial.data.json"
    return open_json(json_file)[0]


# valid
@pytest.fixture
def open_null_spatial_json():
    json_file = JSON_DIR / "null-spatial.data.json"
    return open_json(json_file)[0]
