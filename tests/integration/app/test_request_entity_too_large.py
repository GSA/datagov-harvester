from app.constants import ADMIN_MAX_REQUEST_BYTES, ADMIN_MAX_REQUEST_MB


class TestRequestEntityTooLargeHandler:
    """
    Without this handler APIFlask's json_errors answers browsers with a bare
    JSON blob. See GSA/data.gov#6067. MAX_CONTENT_LENGTH/MAX_FORM_MEMORY_SIZE
    are app-wide (app/__init__.py), so any route that reads request.form
    exercises the same handler - this uses /organization/add rather than
    /validate/, which is GET-only now.
    """

    def test_admin_limit_stays_10mb(self, app):
        """The admin app no longer receives catalogs, so it keeps its own 10MB
        cap rather than the validator's 150MB one (GSA/data.gov#6460)."""
        assert ADMIN_MAX_REQUEST_MB == 10
        assert app.config["MAX_CONTENT_LENGTH"] == ADMIN_MAX_REQUEST_BYTES
        assert app.config["MAX_FORM_MEMORY_SIZE"] == ADMIN_MAX_REQUEST_BYTES

    def test_html_route_renders_the_error_page(self, client):
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        res = client.post(
            "/organization/add",
            data={"padding": "x" * (ADMIN_MAX_REQUEST_BYTES + 1024)},
            content_type="multipart/form-data",
        )

        assert res.status_code == 413
        assert res.content_type.startswith("text/html")
        assert f"must be {ADMIN_MAX_REQUEST_MB}MB or less" in res.text
        # rendered through base.html, not a bare APIFlask response
        assert "Return to the JSON Schema Validator" in res.text

    def test_api_route_returns_json(self, client):
        res = client.post(
            "/api/v1/organization/add",
            data=b'{"name":"' + b"x" * (ADMIN_MAX_REQUEST_BYTES + 1024) + b'"}',
            content_type="application/json",
        )

        assert res.status_code == 413
        assert res.content_type.startswith("application/json")
        # matches the {"error": ...} shape the rest of app/api uses
        assert res.get_json() == {
            "error": f"Submission too large - must be {ADMIN_MAX_REQUEST_MB}MB or less."
        }
