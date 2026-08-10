from search.config import KEYWORD_NORMALIZER, TEXT_ANALYZER

MAPPINGS = {
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
            "dynamic": False,
            "properties": {
                "modified": {"type": "keyword"},
                "issued": {"type": "keyword"},
                "isPartOf": {"type": "keyword"},
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
            "fields": {
                "raw": {"type": "keyword"},
                "normalized": {
                    "type": "keyword",
                    "normalizer": KEYWORD_NORMALIZER,
                },
            },
        },
        "keyword": {
            "type": "text",
            "analyzer": TEXT_ANALYZER,
            "search_analyzer": TEXT_ANALYZER,
            "fields": {
                "raw": {"type": "keyword"},
                "normalized": {
                    "type": "keyword",
                    "normalizer": KEYWORD_NORMALIZER,
                },
            },
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
        "has_download": {"type": "boolean"},
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
        "distribution_titles": {
            "type": "text",
            "analyzer": TEXT_ANALYZER,
            "search_analyzer": TEXT_ANALYZER,
        },
        "spatial_shape": {"type": "geo_shape", "ignore_malformed": True},
        "spatial_centroid": {"type": "geo_point"},
    }
}
