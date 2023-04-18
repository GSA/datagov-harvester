from datagovharvester import __version__
from datagovharvester.example import hello
from utils.json_utilities import open_json
from pathlib import Path
import jsonschema
import pytest

TESTS_DIR = Path(__file__).parents[0]


def test_version():
    assert __version__ == "0.1.0"


def test_hello():
    assert hello('name') == "Hello name!"


@pytest.mark.parametrize(
    "schema, dataset, is_valid",
    open_json(TESTS_DIR / "fixtures" / "parametrizers" /
              "params.json")["test_validate_dcat_us"]
)
def test_dataset_validity(schema, dataset, is_valid, request):

    dataset_schema = request.getfixturevalue(schema)
    file_path = TESTS_DIR / "fixtures" / "jsons" / dataset
    json_data = open_json(file_path)

    output = None

    try:
        jsonschema.validate(json_data, schema=dataset_schema)
        output = is_valid
    except jsonschema.ValidationError as e:
        error_message = "error: " + e.message + ". offending element: " + e.json_path
        print(error_message)
        output = not is_valid

    assert output
