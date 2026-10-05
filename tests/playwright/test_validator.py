import os

import pytest
from playwright.sync_api import expect

is_prod = os.getenv("FLASK_ENV") == "production"

# /validate/ submits via a same-origin fetch() to /api/v1/validate, which only
# the local nginx proxy (docker-compose's `proxy` service, PROXY_PORT) knows
# to route to the validator container - the bare Flask dev server (base_url)
# doesn't have that route at all.
VALIDATOR_PROXY_URL = f"http://localhost:{os.getenv('PROXY_PORT', '8082')}/validate/"


@pytest.fixture()
def upage(unauthed_page):
    unauthed_page.goto(VALIDATOR_PROXY_URL)
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

    def test_ui_validate_by_json_dcatus3(
        self, upage, dcatus_3_catalog_missing_identifier
    ):
        """
        basic run of the validator form of a dcatus 3.0 catalog fetching via json text
        """

        upage.locator("select[name=fetch_method]").select_option("paste")

        upage.locator("select[name=schema]").select_option("dcatus3.0 catalog")

        # add a test dcatus doc
        upage.locator("textarea[name=json_text]").fill(
            dcatus_3_catalog_missing_identifier
        )

        upage.locator("input[type=submit]").click()

        # error table should be visible and with 5 validation errors
        expect(upage.locator(".error-list")).to_be_visible()
        expect(upage.locator(".error-block")).to_have_count(1)

    def test_download_button_triggers_csv(self, upage, dcatus_many_invalid_json):
        """
        With more errors than the 10 shown, the download button should still
        export all of them as a CSV file.
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

    def test_ui_upload_field_hidden_by_default(self, upage):
        """
        Upload field should not be visible when URL is the default fetch method.
        """
        expect(upage.locator("select[name=fetch_method]")).to_have_value("url")
        expect(upage.locator("#upload_field")).not_to_be_visible()

    def test_ui_upload_field_shown_on_selection(self, upage):
        """
        Selecting the upload fetch method should reveal the upload field
        and hide the url and json fields.
        """
        upage.locator("select[name=fetch_method]").select_option("upload")

        expect(upage.locator("#upload_field")).to_be_visible()
        expect(upage.locator("input[type=file][name=json_file]")).to_be_visible()
        expect(upage.locator("#url_field")).not_to_be_visible()
        expect(upage.locator("#json_field")).not_to_be_visible()

    def test_ui_validate_by_file_upload(
        self, upage, dcatus_long_description_json, tmp_path
    ):
        """
        Uploading a valid .json file should run validation and surface errors,
        matching the behaviour of the paste method for the same content.
        """
        json_file = tmp_path / "catalog.json"
        json_file.write_text(dcatus_long_description_json, encoding="utf-8")

        upage.locator("select[name=fetch_method]").select_option("upload")
        upage.locator("input[type=file][name=json_file]").set_input_files(
            str(json_file)
        )
        upage.locator("input[type=submit]").click()

        expect(upage.locator(".error-list")).to_be_visible()
        expect(upage.locator(".error-block")).to_have_count(1)

    def test_ui_upload_rejects_non_json_extension(self, upage, tmp_path):
        """
        Uploading a non-.json file should surface a field validation error
        and not run the validator.
        """
        bad_file = tmp_path / "catalog.txt"
        bad_file.write_text('{"dataset": []}', encoding="utf-8")

        upage.locator("select[name=fetch_method]").select_option("upload")
        upage.locator("input[type=file][name=json_file]").set_input_files(str(bad_file))
        upage.locator("input[type=submit]").click()

        expect(upage.locator("#upload_field .usa-error-message")).to_have_text(
            "Only .json files are accepted."
        )
        expect(upage.locator(".error-list")).not_to_be_visible()

    def test_ui_upload_invalid_json_content(self, upage, tmp_path):
        """
        Uploading a .json file whose content is not valid JSON should surface
        a field error and not run the validator.
        """
        bad_file = tmp_path / "catalog.json"
        bad_file.write_text("this is not { valid json", encoding="utf-8")

        upage.locator("select[name=fetch_method]").select_option("upload")
        upage.locator("input[type=file][name=json_file]").set_input_files(str(bad_file))
        upage.locator("input[type=submit]").click()

        # Position only - the decoder's own message is deliberately not shown,
        # see invalid_json_message in GSA/datagov-validator.
        expect(upage.locator("#upload_field .usa-error-message")).to_have_text(
            "Invalid JSON at line 1, column 1."
        )
        expect(upage.locator(".error-list")).not_to_be_visible()

    def test_ui_shows_the_upload_size_limit(self, upage):
        """The form advertises the limit it enforces."""
        upage.locator("select[name=fetch_method]").select_option("upload")
        expect(upage.locator("#json_file-hint")).to_have_text("Maximum size: 10 MB.")

        upage.locator("select[name=fetch_method]").select_option("paste")
        expect(upage.locator("#json_text-hint")).to_have_text("Maximum size: 10 MB.")

    def test_ui_upload_rejects_file_exceeding_size_limit(self, upage):
        """Refused client-side: inline error, nothing uploaded."""
        upage.locator("select[name=fetch_method]").select_option("upload")
        upage.locator("input[type=file][name=json_file]").set_input_files(
            {
                "name": "large.json",
                "mimeType": "application/json",
                "buffer": b"x" * (11 * 1024 * 1024),  # 11MB, over the 10MB cap
            }
        )

        # survives only if the page never navigated
        upage.evaluate("window.__sameDocument = true")
        upage.locator("input[type=submit]").click()

        expect(upage.locator("#upload_field .usa-error-message")).to_have_text(
            "File is too large. Maximum size is 10 MB."
        )
        assert upage.evaluate("window.__sameDocument") is True

    def test_ui_paste_rejects_json_exceeding_size_limit(self, upage):
        """Same guard on the paste path, measured in bytes."""
        upage.locator("select[name=fetch_method]").select_option("paste")
        upage.evaluate(
            "document.getElementById('json_text').value = 'x'.repeat(11 * 1024 * 1024)"
        )

        upage.evaluate("window.__sameDocument = true")
        upage.locator("input[type=submit]").click()

        expect(upage.locator("#json_field .usa-error-message")).to_have_text(
            "Pasted JSON is too large. Maximum size is 10 MB."
        )
        assert upage.evaluate("window.__sameDocument") is True

    def test_ui_paste_near_the_limit_reaches_the_validator(self, upage):
        """
        A document under the 10MB cap is sent JSON-escaped inside json_text,
        which can grow the request past the proxy's general 12M body limit;
        the validate route has to allow more (proxy/nginx-common.conf,
        nginx-local-proxy.conf) or nginx refuses it before the validator sees it.
        """
        upage.locator("select[name=fetch_method]").select_option("paste")
        # ~8.5MB of mostly quotes, so escaping makes the request ~14MB
        upage.evaluate(
            """document.getElementById('json_text').value =
                '{"dataset": [], "padding": [' + '"",'.repeat(2900000) + '""]}'"""
        )
        upage.locator("input[type=submit]").click()

        expect(upage.locator("#validator-results h2")).to_have_text(
            "Validation results", timeout=30000
        )
        expect(upage.locator("#json_field .usa-error-message")).to_have_count(0)

    def test_post_bypassing_the_browser_is_rejected(self, upage):
        """
        A request that skips the client-side guard entirely can't reach any
        validation code - /validate/ only accepts GET now, so this can't be
        used to tie up datagov-harvest's own workers (GSA/data.gov#6293).
        """
        res = upage.request.post(
            VALIDATOR_PROXY_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=b"json_text=" + b"x" * (11 * 1024 * 1024),
        )

        assert res.status == 405

    @pytest.mark.parametrize(
        "method,field,message",
        [
            ("url", "#url_field", "URL is required."),
            ("paste", "#json_field", "JSON input is required."),
            ("upload", "#upload_field", "A JSON file is required."),
        ],
    )
    def test_ui_empty_input_is_refused_before_submitting(
        self, upage, method, field, message
    ):
        requests = []
        upage.on("request", lambda req: requests.append(req.url))
        upage.locator("select[name=fetch_method]").select_option(method)
        upage.locator("input[type=submit]").click()

        expect(upage.locator(f"{field} .usa-error-message")).to_have_text(message)
        assert not [url for url in requests if url.endswith("/api/v1/validate")]

    def test_ui_upload_utf16_file(self, upage, dcatus_long_description_json, tmp_path):
        """UTF-16 with a BOM decodes the way json.loads(bytes) used to accept it."""
        json_file = tmp_path / "catalog.json"
        json_file.write_bytes(dcatus_long_description_json.encode("utf-16"))

        upage.locator("select[name=fetch_method]").select_option("upload")
        upage.locator("input[type=file][name=json_file]").set_input_files(
            str(json_file)
        )
        upage.locator("input[type=submit]").click()

        expect(upage.locator(".error-block")).to_have_count(1)

    def test_ui_dcatus3_info(self, upage):
        """
        checks to see if the dcatus 3.0 validator info section is visible to the user
        on the validator page
        """
        expect(upage.locator("div[id=dcatus3-validator-info]")).to_be_visible()


class TestValidatorApiResponses:
    """
    How each kind of /api/v1/validate answer is shown, with the API stubbed so
    the answers that are hard to provoke for real (nginx's own 413, a 502,
    a dropped connection) are covered too.
    """

    UNAVAILABLE = "The validator is unavailable right now. Please try again later."

    def _submit_url(self, upage):
        upage.locator("input[name=url]").fill("https://example.gov/data.json")
        upage.locator("input[type=submit]").click()

    @pytest.mark.parametrize(
        "status,content_type,body,message",
        [
            (
                400,
                "application/json",
                '{"error": "Access to private/internal addresses is not allowed."}',
                "Access to private/internal addresses is not allowed.",
            ),
            (
                422,
                "application/json",
                '{"message": "Validation error", '
                '"detail": {"json": {"url": ["Not a valid URL."]}}}',
                "Not a valid URL.",
            ),
            (
                413,
                "text/html",
                "<html><body><h1>413 Request Entity Too Large</h1></body></html>",
                "Submission is too large. Maximum size is 10 MB.",
            ),
            (502, "text/html", "<html>Bad Gateway</html>", UNAVAILABLE),
            (500, "application/json", '{"error": "internal detail"}', UNAVAILABLE),
            (200, "application/json", '{"unexpected": "shape"}', UNAVAILABLE),
        ],
    )
    def test_answer_is_shown_beside_the_field(
        self, upage, status, content_type, body, message
    ):
        upage.route(
            "**/api/v1/validate",
            lambda route: route.fulfill(
                status=status, content_type=content_type, body=body
            ),
        )
        self._submit_url(upage)

        expect(upage.locator("#url_field .usa-error-message")).to_have_text(message)
        expect(upage.locator(".error-list")).to_have_count(0)

    def test_dropped_connection_is_reported_generically(self, upage):
        upage.route("**/api/v1/validate", lambda route: route.abort())
        self._submit_url(upage)

        expect(upage.locator("#url_field .usa-error-message")).to_have_text(
            self.UNAVAILABLE
        )

    def test_results_are_rendered_as_text(self, upage):
        """Identifiers and messages come from the submitted document."""
        upage.route(
            "**/api/v1/validate",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body='{"validation_errors": ['
                "[0, \"$, 'identifier' is a required property\"], "
                '["<img src=x onerror=alert(1)>", "<b>bold</b>"]]}',
            ),
        )
        self._submit_url(upage)

        blocks = upage.locator(".error-block")
        expect(blocks).to_have_count(2)
        expect(blocks.nth(0)).to_contain_text("0 (dataset position)")
        expect(blocks.nth(1)).to_contain_text("<img src=x onerror=alert(1)>")
        expect(blocks.nth(1)).to_contain_text("<b>bold</b>")
        expect(upage.locator("#validator-results img")).to_have_count(0)

    def test_fetch_method_is_locked_while_validating(self, upage):
        pending = []
        upage.route("**/api/v1/validate", lambda route: pending.append(route))
        self._submit_url(upage)

        expect(upage.locator("select[name=fetch_method]")).to_be_disabled()
        expect(upage.locator("input[type=submit]")).to_be_disabled()

        pending[0].fulfill(
            status=200,
            content_type="application/json",
            body='{"validation_errors": []}',
        )
        expect(upage.locator("#validator-results")).to_contain_text(
            "No validation errors found"
        )
        expect(upage.locator("select[name=fetch_method]")).to_be_enabled()
