from datagovharvester import __version__
from datagovharvester.example import hello
from tests.fixtures.data import get_catalog_schema
from utils.json_utilities import open_json
from pathlib import Path
from jsonschema import validate
import pytest


def test_version():
    assert __version__ == "0.1.0"


def test_hello():
    assert hello('name') == "Hello name!"


@pytest.mark.parametrize(
    "schema, dataset, is_valid",
    [
        ('get_catalog_schema', 'numerical-title.data.json', False),
        ('get_catalog_schema', 'collection-1-parent-2-children.data.json', True),
        ('get_catalog_schema', 'missing-catalog.data.json', False),
        ('get_catalog_schema', 'ny.data.json', False),
        ('get_catalog_schema', 'missing-identifier-title.data.json', False),
        ('get_catalog_schema', 'missing-dataset-fields.data.json', False),
        ('get_catalog_schema', 'usda.gov.data.json', True),
        ('get_catalog_schema', 'arm.data.json', True),
        ('get_catalog_schema', 'large-spatial.data.json', True),
        ('get_catalog_schema', 'reserved-title.data.json', True),
        ('get_catalog_schema', 'collection-2-parent-4-children.data.json', True),
        ('get_catalog_schema', 'geospatial.data.json', True),
        ('get_catalog_schema', 'null-spatial.data.json', True)
    ]
)
def test_dataset_validity(schema, dataset, is_valid, request):

    dataset_schema = request.getfixturevalue(schema)
    file_path = Path(__file__).parents[0] / "fixtures" / "jsons" / dataset
    json_data = open_json(file_path)

    try:
        validate(json_data, schema=dataset_schema)
        assert is_valid
    except:
        assert not is_valid
