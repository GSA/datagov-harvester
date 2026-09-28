"""Integration coverage for content-quality warnings on the validate page and
API route (GSA/data.gov#6127).

Schema validation alone can pass a catalog that still trips warnings once
harvested (duplicate identifiers, NARA vocabulary, spatial resolution, etc.).
These tests confirm both `/validate/` (HTML) and `/api/validate` (JSON) now
surface that second layer instead of only ever showing schema errors.
"""

import json


def _paste_form(json_text, schema="dcatus3.0 catalog"):
    return {
        "schema": schema,
        "fetch_method": "paste",
        "json_text": json_text,
    }


def _minimal_dataset(identifier, **extra):
    return {
        "@type": "Dataset",
        "identifier": identifier,
        "title": "t",
        "description": "d",
        "contactPoint": {
            "@type": "Kind",
            "fn": "Test",
            "hasEmail": "mailto:t@example.gov",
        },
        **extra,
    }


_CATALOG_WITH_WARNINGS = json.dumps(
    {
        "@type": "Catalog",
        "dataset": [
            _minimal_dataset("dup-id"),
            _minimal_dataset("dup-id"),
            _minimal_dataset("record-2", language=["us"]),
        ],
    }
)

_CLEAN_CATALOG = json.dumps(
    {
        "@type": "Catalog",
        "dataset": [_minimal_dataset("clean-record")],
    }
)


class TestValidatorPageWarnings:
    def test_html_page_renders_warnings_alongside_errors(self, app, client):
        app.config.update({"WTF_CSRF_ENABLED": False})
        res = client.post(
            "/validate/",
            data=_paste_form(_CATALOG_WITH_WARNINGS),
            content_type="multipart/form-data",
        )

        assert res.status_code == 200
        assert b"No validation errors found" in res.data
        assert b"Content-quality warnings" in res.data
        assert b"duplicate_identifier" in res.data
        assert b"dup-id" in res.data
        assert b"invalid_language" in res.data

    def test_html_page_shows_no_warnings_for_clean_catalog(self, app, client):
        app.config.update({"WTF_CSRF_ENABLED": False})
        res = client.post(
            "/validate/",
            data=_paste_form(_CLEAN_CATALOG),
            content_type="multipart/form-data",
        )

        assert res.status_code == 200
        assert b"No content-quality warnings found" in res.data

    def test_html_page_hides_warnings_section_for_1_1_schema(self, app, client):
        """1.1 schemas have no warning detection today; the section shouldn't
        render at all rather than show a possibly-confusing empty state for a
        schema where nothing was ever checked."""
        app.config.update({"WTF_CSRF_ENABLED": False})
        catalog = json.dumps(
            {"dataset": [{"title": "t", "description": "d", "identifier": "i"}]}
        )
        res = client.post(
            "/validate/",
            data=_paste_form(catalog, schema="dcatus1.1: federal dataset"),
            content_type="multipart/form-data",
        )

        assert res.status_code == 200
        assert b"Content-quality warnings" not in res.data


class TestValidatorApiWarnings:
    def test_api_returns_warnings_for_dcatus3_catalog(self, client):
        res = client.post(
            "/api/v1/validate",
            json={
                "schema": "dcatus3.0 catalog",
                "fetch_method": "paste",
                "json_text": _CATALOG_WITH_WARNINGS,
            },
        )

        assert res.status_code == 200
        body = res.get_json()
        assert body["validation_errors"] == []

        warning_types = {w[1] for w in body["validation_warnings"]}
        assert "duplicate_identifier" in warning_types
        assert "invalid_language" in warning_types

    def test_api_accepts_dcatus3_schema_choice(self, client):
        """dcatus3.0 catalog was previously missing from the schema's allowed
        choices, so a validation-3.0 request would 422 before ever reaching
        validate_records."""
        res = client.post(
            "/api/v1/validate",
            json={
                "schema": "dcatus3.0 catalog",
                "fetch_method": "paste",
                "json_text": _CLEAN_CATALOG,
            },
        )

        assert res.status_code == 200
