from search.queries.registry import (
    build_sort_clause,
    parse_search_query,
)


def test_relevance_sort_uses_popularity_tie_breaker():

    sort_clause = build_sort_clause(sort_by="relevance")

    assert sort_clause == [
        {"_score": {"order": "desc"}},
        {"popularity": {"order": "desc", "missing": "_last"}},
        {"_id": {"order": "desc"}},
    ]


def test_distance_sort_uses_geo_distance():
    sort_clause = build_sort_clause(
        sort_by="distance", sort_point={"lat": 40.0, "lon": -75.0}
    )

    assert sort_clause[0]["_geo_distance"]["spatial_centroid"] == {
        "lat": 40.0,
        "lon": -75.0,
    }
    assert sort_clause[0]["_geo_distance"]["order"] == "asc"


def test_last_harvested_date_sort_uses_latest_first():
    sort_clause = build_sort_clause(sort_by="last_harvested_date")

    assert sort_clause == [
        {"last_harvested_date": {"order": "desc", "missing": "_last"}},
        {"_score": {"order": "desc"}},
        {"popularity": {"order": "desc", "missing": "_last"}},
        {"_id": {"order": "desc"}},
    ]


class TestPhraseAndOrQueryParsing:
    """Test the _parse_search_query method for phrases and OR operators."""

    def test_parse_simple_phrase(self):
        """Test parsing a simple phrase search."""
        result = parse_search_query('"health food"')
        assert result is not None
        assert result["has_or"] is False
        assert len(result["terms"]) == 1
        assert result["terms"][0]["text"] == "health food"
        assert result["terms"][0]["type"] == "phrase"

    def test_parse_simple_or_query(self):
        """Test parsing a simple OR query with two terms."""
        result = parse_search_query("health OR education")
        assert result is not None
        assert result["has_or"] is True
        assert len(result["terms"]) == 2
        assert result["terms"][0]["text"] == "health"
        assert result["terms"][0]["type"] == "term"
        assert result["terms"][1]["text"] == "education"
        assert result["terms"][1]["type"] == "term"

    def test_parse_or_query_with_quoted_phrases(self):
        """Test parsing OR query with quoted phrases."""
        result = parse_search_query('"climate change" OR "global warming"')
        assert result is not None
        assert result["has_or"] is True
        assert len(result["terms"]) == 2
        assert result["terms"][0]["text"] == "climate change"
        assert result["terms"][0]["type"] == "phrase"
        assert result["terms"][1]["text"] == "global warming"
        assert result["terms"][1]["type"] == "phrase"

    def test_parse_or_query_mixed_quotes_and_terms(self):
        """Test parsing OR query with mix of quoted phrases and simple terms."""
        result = parse_search_query('"climate change" OR warming OR environment')
        assert result is not None
        assert result["has_or"] is True
        assert len(result["terms"]) == 3
        assert result["terms"][0]["text"] == "climate change"
        assert result["terms"][0]["type"] == "phrase"
        assert result["terms"][1]["text"] == "warming"
        assert result["terms"][1]["type"] == "term"
        assert result["terms"][2]["text"] == "environment"
        assert result["terms"][2]["type"] == "term"

    def test_parse_no_or_operator_returns_none(self):
        """Test that simple queries without OR or quotes return None."""
        result = parse_search_query("health education")
        assert result is None

    def test_parse_empty_query_returns_none(self):
        """Test that empty query returns None."""
        result = parse_search_query("")
        assert result is None

    def test_parse_none_query_returns_none(self):
        """Test that None query returns None."""
        result = parse_search_query(None)
        assert result is None

    def test_parse_case_insensitive_or(self):
        """Test that OR operator is case insensitive."""
        result = parse_search_query("health or education")
        assert result is not None
        assert result["has_or"] is True
        assert len(result["terms"]) == 2
