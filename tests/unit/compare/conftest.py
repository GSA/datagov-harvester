import pytest
from pathlib import Path
from harvester.utils.json import open_json
from harvester.utils.util import sort_dataset, dataset_to_hash

TEST_DIR = Path(__file__).parents[2]
HARVEST_SOURCES = TEST_DIR / "harvest-sources"


@pytest.fixture
def artificial_data_sources():
    harvest_source = {
        "1": "de955c1b-fa16-4b84-ad6c-f891ba276056",  # update
        "2": "6d500ebc-19f8-4541-82b0-f02ad24c82e3",  # do nothing
        "3": "9aeef506-fbc4-42e4-ad27-c2e7e9f0d1c5",  # create
    }

    ckan_source = {
        "1": "fcd3428b-0ba7-48da-951d-fe44606be556",
        "2": "6d500ebc-19f8-4541-82b0-f02ad24c82e3",
        "4": "dae9b42c-cfc5-4f71-ae97-a5b75234b14f",  # delete
    }

    return harvest_source, ckan_source


@pytest.fixture
def data_sources():
    harvest_source_datasets = open_json(
        HARVEST_SOURCES / "dcatus" / "dcatus_compare.json"
    )["dataset"]

    harvest_source = {}
    for d in harvest_source_datasets:
        harvest_source[d["identifier"]] = dataset_to_hash(
            sort_dataset(d)
        )  # the extract needs to be sorted

    ckan_source_datasets = open_json(
        HARVEST_SOURCES / "dcatus" / "ckan_datasets_resp.json"
    )["result"]["results"]

    ckan_source = {}

    for d in ckan_source_datasets:
        orig_meta = None
        orig_id = None
        for e in d["extras"]:
            if e["key"] == "dcat_metadata":
                orig_meta = eval(e["value"], {"__builtins__": {}})
            if e["key"] == "identifier":
                orig_id = e["value"]

        ckan_source[orig_id] = dataset_to_hash(
            orig_meta
        )  # the response is stored sorted

    return harvest_source, ckan_source
