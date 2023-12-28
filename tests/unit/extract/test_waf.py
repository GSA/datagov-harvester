from harvester import HarvestSource
from harvester.extract import download_waf, traverse_waf


def test_traverse_waf(waf_example: HarvestSource):
    files = traverse_waf(waf_example.url, filters=["../", "dcatus/"])
    assert len(files) == 7

    downloaded_files = download_waf(files)
    assert len(downloaded_files) == 7
