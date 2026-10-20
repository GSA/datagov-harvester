from app.constants import MAX_UPLOAD_BYTES, MAX_UPLOAD_MB


class TestRequestEntityTooLargeHandler:
    """
    Without this handler APIFlask's json_errors answers browsers with a bare
    JSON blob. See GSA/data.gov#6067. MAX_CONTENT_LENGTH/MAX_FORM_MEMORY_SIZE
    are app-wide (app/__init__.py), so any route that reads request.form
    exercises the same handler - this uses /organization/add rather than
    /validate/, which is GET-only now.
    """

    def test_html_route_renders_the_error_page(self, client):
        with client.session_transaction() as sess:
            sess["user"] = "tester@gsa.gov"

        res = client.post(
            "/organization/add",
            data={"padding": "x" * (MAX_UPLOAD_BYTES + 1024)},
            content_type="multipart/form-data",
        )

        assert res.status_code == 413
        assert res.content_type.startswith("text/html")
        assert f"must be {MAX_UPLOAD_MB}MB or less" in res.text
        # rendered through base.html, not a bare APIFlask response
        assert "Return to the JSON Schema Validator" in res.text

    def test_api_route_returns_json(self, client):
        res = client.post(
            "/api/v1/organization/add",
            data=b'{"name":"' + b"x" * (MAX_UPLOAD_BYTES + 1024) + b'"}',
            content_type="application/json",
        )

        assert res.status_code == 413
        assert res.content_type.startswith("application/json")
        # matches the {"error": ...} shape the rest of app/api uses
        assert res.get_json() == {
            "error": f"Submission too large - must be {MAX_UPLOAD_MB}MB or less."
        }
