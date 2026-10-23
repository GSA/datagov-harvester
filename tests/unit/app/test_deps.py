from unittest.mock import Mock, patch

import pytest


def test_get_application_version_from_vcap():
    """Test application version extraction from VCAP_APPLICATION."""
    vcap_json = '{"application_version": "abc123-2026.09.30.1"}'

    with patch.dict("os.environ", {"VCAP_APPLICATION": vcap_json}):
        from app.deps import get_application_version

        version = get_application_version()

        assert version == "2026.09.30.1"


def test_get_application_version_from_git():
    """Test application version extraction from git describe."""
    with patch.dict("os.environ", {}, clear=True):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = Mock(
                returncode=0, stdout="v2026.09.30.1-5-g1234567\n"
            )

            from app.deps import get_application_version

            version = get_application_version()

            assert version == "2026.09.30.1-5-g1234567"


def test_get_application_version_fallback():
    """Test application version fallback when no version available."""
    with patch.dict("os.environ", {}, clear=True):
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = Exception("git not available")

            from app.deps import get_application_version

            version = get_application_version()

            assert version == "unknown"


def test_get_api_version_from_path():
    """Test API version extraction from request path."""
    from app.deps import get_api_version_from_path

    assert get_api_version_from_path("/api/v1/harvest_jobs/") == "v1"
    assert get_api_version_from_path("/api/v2/harvest_sources/") == "v2"
    assert get_api_version_from_path("/api/harvest_jobs/") == "v1"
    assert get_api_version_from_path("/harvest_jobs/") == "v1"


def test_build_collection_response_structure():
    """Test collection response wrapper creates correct structure."""
    from app.deps import build_collection_response

    data = [{"id": "1", "name": "test1"}, {"id": "2", "name": "test2"}]
    path = "/api/v1/harvest_jobs/"
    query_string = "page=0&per_page=10&order_by=-date_created"
    page = 0
    per_page = 10
    total = 42
    order_by = "-date_created"

    result = build_collection_response(
        data=data,
        path=path,
        query_string=query_string,
        page=page,
        per_page=per_page,
        total=total,
        order_by=order_by,
    )

    assert "data" in result
    assert "meta" in result
    assert "links" in result
    assert result["data"] == data
    assert result["meta"]["page"] == 0
    assert result["meta"]["per_page"] == 10
    assert result["meta"]["total"] == 42
    assert result["meta"]["page_count"] == 5
    assert result["meta"]["order_by"] == "-date_created"
    assert "api_version" in result["meta"]
    assert "application_version" in result["meta"]
    assert "self" in result["links"]
    assert "next" in result["links"]
    assert "previous" in result["links"]
    assert "documentation" in result["links"]
    assert "changelog" in result["links"]


def test_build_collection_response_pagination_metadata():
    """Test pagination metadata calculations."""
    from app.deps import build_collection_response

    data = []
    path = "/api/v1/harvest_jobs/"
    query_string = "page=2&per_page=10"

    result = build_collection_response(
        data=data,
        path=path,
        query_string=query_string,
        page=2,
        per_page=10,
        total=42,
        order_by="asc",
    )

    assert result["meta"]["page"] == 2
    assert result["meta"]["per_page"] == 10
    assert result["meta"]["total"] == 42
    assert result["meta"]["page_count"] == 5


def test_build_collection_response_next_link():
    """Test next link generation."""
    from app.deps import build_collection_response

    data = []
    path = "/api/v1/harvest_jobs/"
    query_string = "page=0&per_page=10"

    result = build_collection_response(
        data=data,
        path=path,
        query_string=query_string,
        page=0,
        per_page=10,
        total=42,
        order_by="asc",
    )

    assert result["links"]["next"] is not None
    assert "page=1" in result["links"]["next"]


def test_build_collection_response_previous_link():
    """Test previous link generation."""
    from app.deps import build_collection_response

    data = []
    path = "/api/v1/harvest_jobs/"
    query_string = "page=2&per_page=10"

    result = build_collection_response(
        data=data,
        path=path,
        query_string=query_string,
        page=2,
        per_page=10,
        total=42,
        order_by="asc",
    )

    assert result["links"]["previous"] is not None
    assert "page=1" in result["links"]["previous"]


def test_build_collection_response_first_page_no_previous():
    """Test first page has no previous link."""
    from app.deps import build_collection_response

    data = []
    path = "/api/v1/harvest_jobs/"
    query_string = "page=0&per_page=10"

    result = build_collection_response(
        data=data,
        path=path,
        query_string=query_string,
        page=0,
        per_page=10,
        total=42,
        order_by="asc",
    )

    assert result["links"]["previous"] is None


def test_build_collection_response_last_page_no_next():
    """Test last page has no next link."""
    from app.deps import build_collection_response

    data = []
    path = "/api/v1/harvest_jobs/"
    query_string = "page=4&per_page=10"

    result = build_collection_response(
        data=data,
        path=path,
        query_string=query_string,
        page=4,
        per_page=10,
        total=42,
        order_by="asc",
    )

    assert result["links"]["next"] is None


def test_build_collection_response_static_links():
    """Test static documentation and changelog links."""
    from app.deps import build_collection_response

    data = []
    path = "/api/v1/harvest_jobs/"
    query_string = "page=0&per_page=10"

    result = build_collection_response(
        data=data,
        path=path,
        query_string=query_string,
        page=0,
        per_page=10,
        total=0,
        order_by="asc",
    )

    assert result["links"]["documentation"] == "/openapi/docs"
    assert result["links"]["changelog"] == "/api/changelog"
