from unittest.mock import Mock, patch

import pytest
import requests

from app.validator_client import (
    VALIDATOR_CONNECT_TIMEOUT_SECONDS,
    VALIDATOR_TIMEOUT_SECONDS,
    ValidatorRefused,
    ValidatorUnavailable,
    validate_catalog,
)

VALIDATOR_API_URL = "http://validator.test/api/v1/validate"


def _api_response(status_code, body=None, json_error=False):
    response = Mock()
    response.status_code = status_code
    if json_error:
        response.json = Mock(side_effect=ValueError("not json"))
    else:
        response.json = Mock(return_value=body)
    return response


@pytest.fixture(autouse=True)
def validator_api_url(monkeypatch):
    monkeypatch.setenv("VALIDATOR_API_URL", VALIDATOR_API_URL)


class TestValidateCatalog:
    @patch("app.validator_client.requests.post")
    def test_url_submission(self, mock_post):
        mock_post.return_value = _api_response(
            200, {"validation_errors": [[0, "$, 'identifier' is a required property"]]}
        )

        errors = validate_catalog(
            "dcatus1.1: federal dataset", "url", url="https://example.gov/data.json"
        )

        assert errors == [[0, "$, 'identifier' is a required property"]]
        mock_post.assert_called_once_with(
            VALIDATOR_API_URL,
            json={
                "schema": "dcatus1.1: federal dataset",
                "fetch_method": "url",
                "url": "https://example.gov/data.json",
            },
            timeout=(VALIDATOR_CONNECT_TIMEOUT_SECONDS, VALIDATOR_TIMEOUT_SECONDS),
        )

    @patch("app.validator_client.requests.post")
    def test_paste_submission(self, mock_post):
        mock_post.return_value = _api_response(200, {"validation_errors": []})

        assert validate_catalog("dcatus3.0 catalog", "paste", json_text="{}") == []
        assert mock_post.call_args.kwargs["json"] == {
            "schema": "dcatus3.0 catalog",
            "fetch_method": "paste",
            "json_text": "{}",
        }

    @pytest.mark.parametrize("status_code", [400, 413, 422])
    @patch("app.validator_client.requests.post")
    def test_refusal_carries_the_validator_message(self, mock_post, status_code):
        mock_post.return_value = _api_response(
            status_code, {"error": "URL did not return JSON."}
        )

        with pytest.raises(ValidatorRefused, match="URL did not return JSON."):
            validate_catalog("dcatus3.0 catalog", "url", url="https://example.gov")

    @patch("app.validator_client.requests.post")
    def test_request_validation_error_is_a_refusal(self, mock_post):
        mock_post.return_value = _api_response(
            422,
            {
                "message": "Validation error",
                "detail": {"json": {"url": ["Not a valid URL."]}},
            },
        )

        with pytest.raises(ValidatorRefused, match="Not a valid URL."):
            validate_catalog("dcatus3.0 catalog", "url", url="http://localhost")

    @pytest.mark.parametrize(
        "response",
        [
            _api_response(500, {"error": "API Validator error"}),
            _api_response(502, json_error=True),
            _api_response(200, {"unexpected": "shape"}),
            _api_response(200, ["not", "a", "dict"]),
            _api_response(422, {"message": "Validation error"}),
            _api_response(404, {"message": "Not Found"}),
        ],
    )
    @patch("app.validator_client.requests.post")
    def test_unexpected_responses_are_unavailable(self, mock_post, response):
        mock_post.return_value = response

        with pytest.raises(ValidatorUnavailable):
            validate_catalog("dcatus3.0 catalog", "paste", json_text="{}")

    @patch(
        "app.validator_client.requests.post",
        side_effect=requests.exceptions.ConnectionError("refused"),
    )
    def test_connection_failure_is_unavailable(self, mock_post):
        with pytest.raises(ValidatorUnavailable, match="request failed"):
            validate_catalog("dcatus3.0 catalog", "paste", json_text="{}")

    @patch("app.validator_client.requests.post")
    def test_missing_configuration_is_unavailable(self, mock_post, monkeypatch):
        monkeypatch.delenv("VALIDATOR_API_URL")

        with pytest.raises(ValidatorUnavailable, match="VALIDATOR_API_URL"):
            validate_catalog("dcatus3.0 catalog", "paste", json_text="{}")
        mock_post.assert_not_called()
