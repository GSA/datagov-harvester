from search.config import KEYWORD_NORMALIZER, SETTINGS, TEXT_ANALYZER
from search.mappings import MAPPINGS


def test_title_supports_case_insensitive_sort_without_changing_text_search():
    title = MAPPINGS["properties"]["title"]
    assert title["type"] == "text"
    assert title["analyzer"] == title["search_analyzer"] == TEXT_ANALYZER
    assert title["fields"]["normalized"] == {
        "type": "keyword",
        "normalizer": KEYWORD_NORMALIZER,
    }
    assert SETTINGS["analysis"]["normalizer"][KEYWORD_NORMALIZER]["filter"] == [
        "lowercase"
    ]


def test_has_download_field_mapping():
    assert MAPPINGS["properties"]["has_download"]["type"] == "boolean"


def test_access_level_field_mapping():
    assert MAPPINGS["properties"]["access_level"]["type"] == "keyword"
