import json

from app.constants import MAX_UPLOAD_BYTES, MAX_UPLOAD_MB

# Flask 3.1's MAX_FORM_MEMORY_SIZE default, which used to cap pasted JSON far
# below the advertised limit.
FLASK_DEFAULT_FORM_MEMORY_SIZE = 500_000


def _paste_form(json_text):
    return {
        "schema": "dcatus1.1: federal dataset",
        "fetch_method": "paste",
        "json_text": json_text,
    }


class TestValidatorUploadLimits:
    """
    Every submission method must enforce the one limit the page advertises.
    See GSA/data.gov#6067.
    """

    def test_page_hands_the_limit_to_the_client_side_guard(self, client):
        """
        Jinja renders an undefined variable as "", silently breaking the guard's
        JS. Pin both uses of the limit.
        """
        res = client.get("/validate/")

        assert res.status_code == 200
        assert f"const MAX_UPLOAD_BYTES = {MAX_UPLOAD_BYTES};" in res.text
        assert f"Maximum size: {MAX_UPLOAD_MB} MB." in res.text

    def test_pasted_json_over_flask_form_default_is_accepted(self, app, client):
        padding = "x" * (2 * 1024 * 1024)
        catalog = json.dumps({"dataset": [], "padding": padding})
        assert FLASK_DEFAULT_FORM_MEMORY_SIZE < len(catalog) < MAX_UPLOAD_BYTES

        app.config.update({"WTF_CSRF_ENABLED": False})
        res = client.post(
            "/validate/",
            data=_paste_form(catalog),
            content_type="multipart/form-data",
        )

        assert res.status_code == 200
        # the form was processed, not re-rendered blank
        assert b"No validation errors found" in res.data

    def test_pasted_json_over_the_upload_limit_is_rejected(self, app, client):
        oversized = "x" * (MAX_UPLOAD_BYTES + 1024)

        app.config.update({"WTF_CSRF_ENABLED": False})
        res = client.post(
            "/validate/",
            data=_paste_form(oversized),
            content_type="multipart/form-data",
        )

        assert res.status_code == 413


class TestRequestEntityTooLargeHandler:
    """
    Without this handler APIFlask's json_errors answers browsers with a bare JSON
    blob. See GSA/data.gov#6067.
    """

    def test_html_route_renders_the_error_page(self, app, client):
        app.config.update({"WTF_CSRF_ENABLED": False})
        res = client.post(
            "/validate/",
            data=_paste_form("x" * (MAX_UPLOAD_BYTES + 1024)),
            content_type="multipart/form-data",
        )

        assert res.status_code == 413
        assert res.content_type.startswith("text/html")
        assert f"must be {MAX_UPLOAD_MB}MB or less" in res.text
        # rendered through base.html, not a bare APIFlask response
        assert "Return to the JSON Schema Validator" in res.text

    def test_api_route_returns_json(self, app, client):
        res = client.post(
            "/api/v1/validate",
            data=b'{"json_text":"' + b"x" * (MAX_UPLOAD_BYTES + 1024) + b'"}',
            content_type="application/json",
        )

        assert res.status_code == 413
        assert res.content_type.startswith("application/json")
        # matches the {"error": ...} shape the rest of app/api uses
        assert res.get_json() == {
            "error": f"Submission too large - must be {MAX_UPLOAD_MB}MB or less."
        }
