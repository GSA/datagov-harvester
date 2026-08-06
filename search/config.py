DEFAULT_PER_PAGE = 20
DEFAULT_PAGE = 1
SEARCH_API_MAX_PER_PAGE = 1000

DEFAULT_TIMEOUT_RETRIES = 3
DEFAULT_TIMEOUT_BACKOFF_BASE = 2.0
DEFAULT_DELETE_REQUEST_TIMEOUT_SECONDS = 120
DEFAULT_REFRESH_REQUEST_TIMEOUT_SECONDS = 120
DEFAULT_CLIENT_MAX_RETRIES = 3

INDEX_NAME = "datasets"
TEXT_ANALYZER = "datagov_text"
STOP_FILTER = "datagov_stop"
KEYWORD_NORMALIZER = "lowercase_normalizer"
DEFAULT_CATALOG_BASE_URL = "https://catalog.data.gov"

SETTINGS = {
    "analysis": {
        "filter": {
            STOP_FILTER: {
                "type": "stop",
                "stopwords": "_english_",
            }
        },
        "analyzer": {
            TEXT_ANALYZER: {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", STOP_FILTER],
            }
        },
        "normalizer": {
            KEYWORD_NORMALIZER: {
                "type": "custom",
                "filter": ["lowercase"],
            }
        },
    }
}
