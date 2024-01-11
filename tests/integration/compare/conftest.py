import os
from pathlib import Path

import pytest

from harvester import Record, Source
from harvester.load import create_ckan_entrypoint, search_ckan
from harvester.utils.json import open_json
from harvester.utils.util import dataset_to_hash, sort_dataset

TEST_DIR = Path(__file__).parents[2]
HARVEST_SOURCES = TEST_DIR / "harvest-sources"


@pytest.fixture
def ckan_entrypoint():
    catalog_dev_api_key = os.getenv("CKAN_API_TOKEN_DEV")  # gha
    if catalog_dev_api_key is None:  # local
        import credentials

        catalog_dev_api_key = credentials.ckan_catalog_dev_api_key

    return create_ckan_entrypoint("https://catalog-dev.data.gov/", catalog_dev_api_key)


@pytest.fixture
def data_sources(ckan_entrypoint):
    harvest_source_datasets = open_json(
        HARVEST_SOURCES / "dcatus" / "dcatus_compare.json"
    )["dataset"]

    harvest_records = {}
    for dataset in harvest_source_datasets:
        record = Record(
            identifier=dataset["identifier"],
            raw_hash=dataset_to_hash(sort_dataset(dataset)),
        )
        harvest_records[record.identifier] = record
    harvest_source = Source(
        url="dummyurl",
        records=harvest_records,
        source_type="dcatus",
    )

    ckan_source_datasets = search_ckan(
        ckan_entrypoint,
        {
            "q": 'harvest_source_name:"test_harvest_source_name"',
            "fl": [
                "extras_harvest_source_name",
                "extras_dcat_metadata",
                "extras_identifier",
            ],
        },
    )["results"]
    ckan_records = {}
    for dataset in ckan_source_datasets:
        record = Record(
            identifier=dataset["identifier"],
            raw_hash=dataset_to_hash(
                eval(dataset["dcat_metadata"], {"__builtins__": {}})
            ),
        )
        ckan_records[record.identifier] = record
    ckan_source = Source(
        url="ckandummyurl",
        records=ckan_records,
        source_type="ckan",
    )

    return harvest_source, ckan_source
