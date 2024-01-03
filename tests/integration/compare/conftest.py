import os
from pathlib import Path

import pytest

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

    harvest_source = {}
    for d in harvest_source_datasets:
        harvest_source[d["identifier"]] = dataset_to_hash(
            sort_dataset(d)
        )  # the extract needs to be sorted

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

    ckan_source = {}

    for d in ckan_source_datasets:
        ckan_source[d["identifier"]] = dataset_to_hash(
            eval(d["dcat_metadata"], {"__builtins__": {}})
        )  # the response is stored sorted

    return harvest_source, ckan_source
