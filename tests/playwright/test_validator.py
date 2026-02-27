import pytest
import os
from playwright.sync_api import expect

is_prod = os.getenv("FLASK_ENV") == "production"


@pytest.fixture()
def upage(unauthed_page):
    unauthed_page.goto("/validate/")
    yield unauthed_page


class TestValidator:
    def test_ui_validate_by_url(self, upage):
        """
        basic run of the validator form using default values and fetching via url
        """
        # schema
        expect(upage.locator("select[name=schema]")).to_have_value(
            "dcatus1.1: federal dataset"
        )
        # fetch_method
        expect(upage.locator("select[name=fetch_method]")).to_have_value("url")

        # url
        expect(upage.locator("input[name=url]")).to_have_attribute(
            "placeholder", "https://example.com/data.json"
        )
        expect(upage.locator("input[name=url]")).to_have_value("")

        # add a test dcatus doc
        upage.locator("input[name=url]").fill(
            "http://nginx-harvest-source/dcatus/dcatus_multiple_invalid.json"
        )

        upage.locator("input[type=submit]").click()

        # error table should be visible and with 5 validation errors
        expect(upage.locator(".usa-table-container")).to_be_visible()
        expect(upage.locator(".error-block")).to_have_count(5)

    def test_ui_validate_by_json(self, upage, dcatus_long_description_json):
        """
        basic run of the validator form using default values and fetching via json text
        """

        upage.locator("select[name=fetch_method]").select_option("paste")

        # add a test dcatus doc
        upage.locator("textarea[name=json_text]").fill(dcatus_long_description_json)

        upage.locator("input[type=submit]").click()

        # error table should be visible and with 5 validation errors
        expect(upage.locator(".usa-table-container")).to_be_visible()
        expect(upage.locator(".error-block")).to_have_count(1)

    def test_api_validate_by_url(self, upage, validator_api_url):

        res = upage.request.post(
            "/api/validate",
            headers={
                "Content-Type": "application/json",
            },
            data=validator_api_url,
        )

        assert res.status == 200
        assert res.json() == {
            "validation_errors": [[0, "$, 'identifier' is a required property"]]
        }

    def test_api_validate_by_bad_url(self, upage, validator_api_url):

        validator_api_url["url"] = "nonsense"

        res = upage.request.post(
            "/api/validate",
            headers={
                "Content-Type": "application/json",
            },
            data=validator_api_url,
        )

        assert res.status == 422
        assert res.json() == {
            "detail": {"json": {"url": ["Not a valid URL."]}},
            "message": "Validation error",
        }

    def test_api_validate_by_json(self, upage, validator_api_json):

        res = upage.request.post(
            "/api/validate",
            headers={
                "Content-Type": "application/json",
            },
            data=validator_api_json,
        )

        # ruff: noqa: E501
        assert res.status == 200
        assert res.json() == {
            "validation_errors": [
                [
                    "https://www.arcgis.com/home/item.html?id=99731bb0369848169d98f31ce83fb0e2",
                    "$.license, 'center' does not match any of the acceptable formats: 'uri', 'null', '^(\\\\[\\\\[REDACTED).*?(\\\\]\\\\])$'",
                ]
            ]
        }

    def test_api_validate_by_invalid_json(self, upage, validator_api_json):

        validator_api_json["json_text"] = "09u10293u{}d12-901aoi"

        res = upage.request.post(
            "/api/validate",
            headers={
                "Content-Type": "application/json",
            },
            data=validator_api_json,
        )

        # ruff: noqa: E501
        assert res.status == 422
        assert res.json() == {
            "detail": {"json": {"_schema": ["Invalid JSON"]}},
            "message": "Validation error",
        }
