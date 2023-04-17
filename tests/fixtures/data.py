import pytest
from pathlib import Path
from utils.json_utilities import open_json

BASE_DIR = Path(__file__).parents[2]
SCHEMA_DIR = BASE_DIR / "schemas"


@pytest.fixture
def get_dataset_schema():

    dataset_schema = SCHEMA_DIR / "dataset.json"
    return open_json(dataset_schema)


@pytest.fixture
def get_catalog_schema():

    catalog_schema = SCHEMA_DIR / "catalog.json"
    return open_json(catalog_schema)
