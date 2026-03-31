"""Shared OpenSearch schema for the datasets index."""

TEXT_ANALYZER = "datagov_text"
STOP_FILTER = "datagov_stop"

OPENSEARCH_SETTINGS = {
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
    }
}

OPENSEARCH_MAPPINGS = {
    "properties": {
        "title": {
            "type": "text",
            "analyzer": TEXT_ANALYZER,
            "search_analyzer": TEXT_ANALYZER,
        },
        "slug": {"type": "keyword"},
        "last_harvested_date": {"type": "date"},
        "dcat": {
            "type": "nested",
            "properties": {
                "modified": {"type": "keyword"},
                "issued": {"type": "keyword"},
            },
        },
        "description": {
            "type": "text",
            "analyzer": TEXT_ANALYZER,
            "search_analyzer": TEXT_ANALYZER,
        },
        "publisher": {
            "type": "text",
            "analyzer": TEXT_ANALYZER,
            "search_analyzer": TEXT_ANALYZER,
        },
        "keyword": {
            "type": "text",
            "analyzer": TEXT_ANALYZER,
            "search_analyzer": TEXT_ANALYZER,
            "fields": {"raw": {"type": "keyword"}},
        },
        "theme": {
            "type": "text",
            "analyzer": TEXT_ANALYZER,
            "search_analyzer": TEXT_ANALYZER,
        },
        "identifier": {
            "type": "text",
            "analyzer": TEXT_ANALYZER,
            "search_analyzer": TEXT_ANALYZER,
        },
        "has_spatial": {"type": "boolean"},
        "popularity": {"type": "integer"},
        "organization": {
            "type": "nested",
            "properties": {
                "id": {"type": "keyword"},
                "name": {
                    "type": "text",
                    "analyzer": TEXT_ANALYZER,
                    "search_analyzer": TEXT_ANALYZER,
                },
                "description": {
                    "type": "text",
                    "analyzer": TEXT_ANALYZER,
                    "search_analyzer": TEXT_ANALYZER,
                },
                "slug": {"type": "keyword"},
                "organization_type": {"type": "keyword"},
            },
        },
        "spatial_shape": {"type": "geo_shape", "ignore_malformed": True},
        "spatial_centroid": {"type": "geo_point"},
    }
}
