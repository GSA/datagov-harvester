from harvester.extract.waf import traverse_waf, download_waf


def test_traverse_waf(get_waf_url):
    files = traverse_waf(get_waf_url, filters=["../", "dcatus/"])
    assert len(files) == 7

    downloaded_files = download_waf(files)
    assert len(downloaded_files) == 7
