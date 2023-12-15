from harvester.extract.waf import traverse_waf


def test_traverse_waf(get_waf_url):
    files = traverse_waf(get_waf_url, filters=["../", "dcatus/"])
    assert len(files) == 7
