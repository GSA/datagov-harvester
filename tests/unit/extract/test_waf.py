from harvester import Source
from harvester.extract import download_waf, traverse_waf


def test_traverse_waf(waf_example: Source):
    files = traverse_waf(waf_example.url, filters=["../", "dcatus/"])
    assert len(files) == 7

    downloaded_files = download_waf(files)
    assert len(downloaded_files) == 7
