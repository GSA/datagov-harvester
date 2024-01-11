from pathlib import Path

import pytest

from harvester import Record, Source
from harvester.utils.json import open_json
from harvester.utils.util import dataset_to_hash, sort_dataset

TEST_DIR = Path(__file__).parents[2]
HARVEST_SOURCES = TEST_DIR / "harvest-sources"


@pytest.fixture
def artificial_data_sources():
    """TODO what/why is this"""
    # key = dataset identifier
    # value = hash value of the dataset
    records = {
        "1": Record("1", {}, raw_hash="de955c1b-fa16-4b84-ad6c-f891ba276056"),  # update
        "2": Record(
            "2", {}, raw_hash="6d500ebc-19f8-4541-82b0-f02ad24c82e3"
        ),  # do nothing
        "3": Record("3", {}, raw_hash="9aeef506-fbc4-42e4-ad27-c2e7e9f0d1c5"),  # create
    }
    harvest_source = Source("dummyurl", records, "dcatus")

    ckan_records = {
        "1": Record("1", {}, raw_hash="fcd3428b-0ba7-48da-951d-fe44606be556"),
        "2": Record("2", {}, raw_hash="6d500ebc-19f8-4541-82b0-f02ad24c82e3"),
        "4": Record("4", {}, raw_hash="dae9b42c-cfc5-4f71-ae97-a5b75234b14f"),  # delete
    }
    ckan_source = Source("dummyurl", ckan_records, "ckan")

    return harvest_source, ckan_source


@pytest.fixture
def dcatus_compare_json_source():
    harvest_source_datasets = open_json(
        HARVEST_SOURCES / "dcatus" / "dcatus_compare.json"
    )["dataset"]

    harvest_records = {}
    for dataset in harvest_source_datasets:
        record = Record(
            identifier=dataset["identifier"],
            raw_metadata=dataset,
            raw_hash=dataset_to_hash(sort_dataset(dataset)),
        )
        harvest_records[record.identifier] = record
    harvest_source = Source(
        url="dummyurl",
        records=harvest_records,
        source_type="dcatus",
    )

    return harvest_source


@pytest.fixture
def ckan_datasets_resp_json_source():
    """TODO what/why is this"""

    ckan_source_datasets = open_json(
        HARVEST_SOURCES / "dcatus" / "ckan_datasets_resp.json"
    )["result"]["results"]
    ckan_records = {}
    for dataset in ckan_source_datasets:
        orig_meta = None
        orig_id = None
        for extra in dataset["extras"]:
            if extra["key"] == "dcat_metadata":
                orig_meta = eval(extra["value"], {"__builtins__": {}})
            if extra["key"] == "identifier":
                orig_id = extra["value"]
        record = Record(
            identifier=dataset["id"],
            raw_metadata=dataset,
            raw_hash=dataset_to_hash(sort_dataset(orig_meta)),
        )
        ckan_records[orig_id] = record
    ckan_source = Source(
        url="ckandummyurl",
        records=ckan_records,
        source_type="ckan",
    )

    return ckan_source
