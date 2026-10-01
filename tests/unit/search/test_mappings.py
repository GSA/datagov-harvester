from search.mappings import MAPPINGS


def test_has_download_field_mapping():
    assert MAPPINGS["properties"]["has_download"]["type"] == "boolean"


def test_access_level_field_mapping():
    assert MAPPINGS["properties"]["access_level"]["type"] == "keyword"
