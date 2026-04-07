import os

import pytest
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
        expect(upage.locator(".error-list")).to_be_visible()
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
        expect(upage.locator(".error-list")).to_be_visible()
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

    def test_download_button_triggers_csv(self, upage, dcatus_many_invalid_json):
        """
        The download button should only appear when there are more than 10 errors,
        and clicking it should trigger a CSV file download.
        """
        upage.locator("select[name=fetch_method]").select_option("paste")
        upage.locator("textarea[name=json_text]").fill(dcatus_many_invalid_json)
        upage.locator("input[type=submit]").click()

        # Confirm we have more errors than the display cap so the button is present.
        error_blocks = upage.locator(".error-block")
        expect(error_blocks).to_have_count(10)  # only first 10 are rendered
        expect(upage.locator("#btn-download")).to_be_visible()

        # Download triggered by the button click.
        with upage.expect_download() as download_info:
            upage.locator("#btn-download").click()

        download = download_info.value
        assert download.suggested_filename == "validation_errors.csv"

        path = download.path()
        content = path.read_text(encoding="utf-8")
        lines = content.strip().splitlines()

        assert lines[0] == '"Dataset identifier","Error"'
        assert len(lines) > 1, "CSV should contain at least one error row"
        for line in lines[1:]:
            assert line.count('"') >= 4, f"Malformed CSV row: {line}"
