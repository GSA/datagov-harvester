import json
from unittest.mock import Mock

import pytest
import requests

from app.constants import MAX_UPLOAD_BYTES, MAX_UPLOAD_MB
from app.validator_client import VALIDATOR_UNAVAILABLE_MESSAGE

# Flask 3.1's MAX_FORM_MEMORY_SIZE default, which used to cap pasted JSON far
# below the advertised limit.
FLASK_DEFAULT_FORM_MEMORY_SIZE = 500_000

VALIDATOR_API_URL = "http://validator.test/api/v1/validate"

# messages datagov-validator answers with; it owns their wording
NESTING_TOO_DEEP_MESSAGE = (
    "Catalog is nested too deeply to validate. "
    "Flatten the nested catalog or hasPart chains and try again."
)
TIMEOUT_MESSAGE = (
    "The URL took longer than 10 seconds to respond. "
    "Check that the URL is correct and the server is up."
)
PRIVATE_ADDRESS_MESSAGE = "Access to private/internal addresses is not allowed."


def _api_response(status_code, body):
    response = Mock()
    response.status_code = status_code
    response.json = Mock(return_value=body)
    return response


@pytest.fixture
def validator_api(app, monkeypatch):
    """
    Stands in for datagov-validator. Answers "no errors" unless a test sets
    `return_value`/`side_effect`; inspect `call_args` for what the page sent.
    """
    app.config.update({"WTF_CSRF_ENABLED": False})
    monkeypatch.setenv("VALIDATOR_API_URL", VALIDATOR_API_URL)
    post = Mock(return_value=_api_response(200, {"validation_errors": []}))
    monkeypatch.setattr("app.validator_client.requests.post", post)
    return post


def _paste_form(json_text, schema="dcatus1.1: federal dataset"):
    return {
        "schema": schema,
        "fetch_method": "paste",
        "json_text": json_text,
    }


def _url_form(url):
    return {
        "schema": "dcatus1.1: federal dataset",
        "fetch_method": "url",
        "url": url,
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

    def test_pasted_json_over_flask_form_default_is_accepted(
        self, client, validator_api
    ):
        padding = "x" * (2 * 1024 * 1024)
        catalog = json.dumps({"dataset": [], "padding": padding})
        assert FLASK_DEFAULT_FORM_MEMORY_SIZE < len(catalog) < MAX_UPLOAD_BYTES

        res = client.post(
            "/validate/",
            data=_paste_form(catalog),
            content_type="multipart/form-data",
        )

        assert res.status_code == 200
        # the form was processed, not re-rendered blank
        assert b"No validation errors found" in res.data
        assert validator_api.call_args.kwargs["json"]["json_text"] == catalog

    def test_pasted_json_over_the_upload_limit_is_rejected(self, client, validator_api):
        oversized = "x" * (MAX_UPLOAD_BYTES + 1024)

        res = client.post(
            "/validate/",
            data=_paste_form(oversized),
            content_type="multipart/form-data",
        )

        assert res.status_code == 413
        validator_api.assert_not_called()


class TestRequestEntityTooLargeHandler:
    """
    Without this handler APIFlask's json_errors answers browsers with a bare JSON
    blob. See GSA/data.gov#6067.
    """

    def test_html_route_renders_the_error_page(self, client, validator_api):
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


class TestSubmissionsReachTheValidator:
    def test_results_are_rendered(self, client, validator_api):
        validator_api.return_value = _api_response(
            200,
            {
                "validation_errors": [
                    [0, "$, 'identifier' is a required property"],
                    ["abc", "$.title, 'title' is a required property"],
                ]
            },
        )

        res = client.post(
            "/validate/",
            data=_paste_form('{"dataset": [{}]}'),
            content_type="multipart/form-data",
        )

        assert res.status_code == 200
        assert "0 (dataset position)" in res.text
        assert "$, &#39;identifier&#39; is a required property" in res.text
        assert "abc" in res.text

    def test_url_is_sent_for_the_validator_to_fetch(self, client, validator_api):
        res = client.post(
            "/validate/",
            data=_url_form("https://example.com/data.json"),
            content_type="multipart/form-data",
        )

        assert res.status_code == 200
        assert validator_api.call_args.args == (VALIDATOR_API_URL,)
        assert validator_api.call_args.kwargs["json"] == {
            "schema": "dcatus1.1: federal dataset",
            "fetch_method": "url",
            "url": "https://example.com/data.json",
        }

    def test_dcatus3_paste(self, client, validator_api):
        client.post(
            "/validate/",
            data=_paste_form('{"@type": "Catalog"}', schema="dcatus3.0 catalog"),
            content_type="multipart/form-data",
        )

        assert validator_api.call_args.kwargs["json"] == {
            "schema": "dcatus3.0 catalog",
            "fetch_method": "paste",
            "json_text": '{"@type": "Catalog"}',
        }

    def test_upload_is_sent_as_pasted_text(self, client, validator_api):
        from io import BytesIO

        # BOM-prefixed, which json.loads(bytes) accepted when the page parsed it
        document = '{"dataset": []}'
        res = client.post(
            "/validate/",
            data={
                "schema": "dcatus1.1: federal dataset",
                "fetch_method": "upload",
                "json_file": (BytesIO(b"\xef\xbb\xbf" + document.encode()), "c.json"),
            },
            content_type="multipart/form-data",
        )

        assert res.status_code == 200
        assert b"No validation errors found" in res.data
        assert validator_api.call_args.kwargs["json"] == {
            "schema": "dcatus1.1: federal dataset",
            "fetch_method": "paste",
            "json_text": document,
        }


class TestRefusalsAreExplained:
    """
    A submission the validator refuses has to say why, beside the input that
    carried it (GSA/data.gov#6067, GSA/data.gov#6293).
    """

    @pytest.mark.parametrize(
        "status_code,message",
        [
            (400, TIMEOUT_MESSAGE),
            (400, PRIVATE_ADDRESS_MESSAGE),
            (422, NESTING_TOO_DEEP_MESSAGE),
        ],
    )
    def test_url_refusal_shows_the_reason_by_the_field(
        self, client, validator_api, status_code, message
    ):
        validator_api.return_value = _api_response(status_code, {"error": message})

        res = client.post(
            "/validate/",
            data=_url_form("https://example.com/slow.json"),
            content_type="multipart/form-data",
        )

        assert res.status_code == 200
        assert message in res.text
        assert '<span class="usa-error-message" role="alert">' in res.text
        assert "No validation errors found" not in res.text

    def test_paste_refusal_shows_the_reason_by_the_field(self, client, validator_api):
        validator_api.return_value = _api_response(
            422, {"error": NESTING_TOO_DEEP_MESSAGE}
        )

        res = client.post(
            "/validate/",
            data=_paste_form('{"dataset": []}', schema="dcatus3.0 catalog"),
            content_type="multipart/form-data",
        )

        assert res.status_code == 200
        assert NESTING_TOO_DEEP_MESSAGE in res.text
        assert "No validation errors found" not in res.text

    def test_request_the_validator_rejects_is_explained(self, client, validator_api):
        """A URL the form accepts but the validator's own schema doesn't."""
        validator_api.return_value = _api_response(
            422,
            {
                "message": "Validation error",
                "detail": {"json": {"url": ["Not a valid URL."]}},
            },
        )

        res = client.post(
            "/validate/",
            data=_url_form("http://localhost/data.json"),
            content_type="multipart/form-data",
        )

        assert res.status_code == 200
        assert "Not a valid URL." in res.text


class TestValidatorUnavailable:
    """Anything unanticipated is logged and reported generically."""

    @pytest.mark.parametrize(
        "post",
        [
            Mock(side_effect=requests.exceptions.ConnectionError("internal detail")),
            Mock(side_effect=requests.exceptions.Timeout("internal detail")),
            Mock(return_value=_api_response(500, {"error": "internal detail"})),
            Mock(return_value=_api_response(502, None)),
            Mock(return_value=_api_response(200, {"unexpected": "shape"})),
        ],
    )
    def test_failure_is_reported_generically(
        self, client, validator_api, monkeypatch, post
    ):
        monkeypatch.setattr("app.validator_client.requests.post", post)

        res = client.post(
            "/validate/",
            data=_url_form("https://example.com/data.json"),
            content_type="multipart/form-data",
        )

        assert res.status_code == 200
        assert VALIDATOR_UNAVAILABLE_MESSAGE in res.text
        assert "internal detail" not in res.text
        assert "No validation errors found" not in res.text

    def test_missing_configuration_is_reported_generically(
        self, client, validator_api, monkeypatch
    ):
        monkeypatch.delenv("VALIDATOR_API_URL")

        res = client.post(
            "/validate/",
            data=_paste_form('{"dataset": []}'),
            content_type="multipart/form-data",
        )

        assert res.status_code == 200
        assert VALIDATOR_UNAVAILABLE_MESSAGE in res.text
        validator_api.assert_not_called()
