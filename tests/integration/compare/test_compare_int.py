from harvester.load import (
    create_ckan_package,
    purge_ckan_package,
    update_ckan_package,
    dcatus_to_ckan,
)
from harvester.compare import compare
from harvester.utils.json import open_json

from pathlib import Path

TEST_DIR = Path(__file__).parents[3]
HARVEST_SOURCES = TEST_DIR / "harvest-sources"


def test_compare(data_sources):
    compare_res = compare(*data_sources)

    assert len(compare_res["create"]) == 1
    assert len(compare_res["update"]) == 3
    assert len(compare_res["delete"]) == 1
