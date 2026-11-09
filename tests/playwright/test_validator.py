import gc
import json
import os

import pytest
import requests
from playwright.sync_api import expect

from dcatus_validation.fetch import PAYLOAD_TOO_LARGE_MESSAGE
from dcatus_validation.limits import MAX_UPLOAD_BYTES
from tests.utils.large_catalog import FEDERAL, write_catalog

is_prod = os.getenv("FLASK_ENV") == "production"

# /validate/ submits via a same-origin fetch() to /api/v1/validate, which only
# the local nginx proxy (docker-compose's `proxy` service, PROXY_PORT) knows
# to route to the validator container - the bare Flask dev server (base_url)
# doesn't have that route at all.
PROXY_ORIGIN = f"http://localhost:{os.getenv('PROXY_PORT', '8082')}"
VALIDATOR_PROXY_URL = f"{PROXY_ORIGIN}/validate/"
VALIDATE_API_URL = f"{PROXY_ORIGIN}/api/v1/validate"


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
        # see dcatus_validation.fetch.invalid_json_message.
        expect(upage.locator("#upload_field .usa-error-message")).to_have_text(
            "Invalid JSON at line 1, column 1."
        )
        expect(upage.locator(".error-list")).not_to_be_visible()

    def test_ui_shows_the_upload_size_limit(self, upage):
        """The form advertises the limit it enforces."""
        upage.locator("select[name=fetch_method]").select_option("upload")
        expect(upage.locator("#json_file-hint")).to_have_text("Maximum size: 150 MB.")

        upage.locator("select[name=fetch_method]").select_option("paste")
        expect(upage.locator("#json_text-hint")).to_have_text("Maximum size: 150 MB.")
        assert upage.evaluate("window.MAX_UPLOAD_BYTES") == 150 * 1024 * 1024

    def test_ui_upload_rejects_file_exceeding_size_limit(self, upage):
        """Refused client-side: inline error, nothing uploaded. The page's
        limit is lowered so the test needn't push a 150MB file into the
        browser; test_ui_shows_the_upload_size_limit pins the real one."""
        upage.evaluate("window.MAX_UPLOAD_BYTES = 1024")
        upage.locator("select[name=fetch_method]").select_option("upload")
        upage.locator("input[type=file][name=json_file]").set_input_files(
            {
                "name": "large.json",
                "mimeType": "application/json",
                "buffer": b"x" * 1025,  # one byte over the lowered limit
            }
        )

        # survives only if the page never navigated
        upage.evaluate("window.__sameDocument = true")
        upage.locator("input[type=submit]").click()

        expect(upage.locator("#upload_field .usa-error-message")).to_have_text(
            "File is too large. Maximum size is 150 MB."
        )
        assert upage.evaluate("window.__sameDocument") is True

    def test_ui_paste_rejects_json_exceeding_size_limit(self, upage):
        """Same guard on the paste path, measured in bytes: 513 two-byte
        characters are under the lowered limit in characters, over it in
        bytes."""
        upage.evaluate("window.MAX_UPLOAD_BYTES = 1024")
        upage.locator("select[name=fetch_method]").select_option("paste")
        upage.evaluate("document.getElementById('json_text').value = 'é'.repeat(513)")

        upage.evaluate("window.__sameDocument = true")
        upage.locator("input[type=submit]").click()

        expect(upage.locator("#json_field .usa-error-message")).to_have_text(
            "Pasted JSON is too large. Maximum size is 150 MB."
        )
        assert upage.evaluate("window.__sameDocument") is True

    def test_ui_paste_near_the_limit_reaches_the_validator(self, upage):
        """
        A paste bigger than the proxy's general 12M body limit reaches the
        validator: the validate routes allow more (proxy/nginx-common.conf,
        nginx-local-proxy.conf), or nginx would refuse it first.
        """
        upage.locator("select[name=fetch_method]").select_option("paste")
        # ~13MB, sent as the request body itself
        upage.evaluate("""document.getElementById('json_text').value =
                '{"dataset": [], "padding": [' + '"",'.repeat(4400000) + '""]}'""")
        # Encoding ~13MB for the request can outlast the 2.5s default on a
        # slow CI runner.
        upage.locator("input[type=submit]").click(timeout=30000)

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
        assert not [url for url in requests if "/api/v1/validate" in url]

    def test_ui_upload_sends_the_file_itself(
        self, upage, dcatus_long_description_json, tmp_path
    ):
        """GSA/data.gov#6460: the page sends an uploaded file as the request
        body, untouched, so the browser never reads it into a JS string or
        JSON-escapes it."""
        content = dcatus_long_description_json.encode("utf-8")
        json_file = tmp_path / "catalog.json"
        json_file.write_bytes(content)
        sent = []
        upage.on(
            "request",
            lambda req: sent.append(req) if "/api/v1/validate" in req.url else None,
        )

        upage.locator("select[name=fetch_method]").select_option("upload")
        upage.locator("input[type=file][name=json_file]").set_input_files(
            str(json_file)
        )
        upage.locator("input[type=submit]").click()

        expect(upage.locator(".error-block")).to_have_count(1)
        [request] = sent
        assert "/api/v1/validate/document?" in request.url
        assert "fetch_method=upload" in request.url
        # Playwright doesn't expose a File body, but its length shows it went
        # as is: not JSON-escaped inside json_text.
        assert request.all_headers()["content-length"] == str(len(content))

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


class TestValidatorApi:
    """The validate API end to end: proxy -> validator container, including a
    real URL fetch from the compose network."""

    def test_api_validate_by_url(self, upage, validator_api_url):
        res = upage.request.post(VALIDATE_API_URL, data=validator_api_url)

        assert res.status == 200
        assert res.headers["x-served-by"] == "datagov-harvest-validator"
        assert res.json() == {
            "validation_errors": [[0, "$, 'identifier' is a required property"]]
        }

    def test_api_validate_by_bad_url(self, upage, validator_api_url):
        validator_api_url["url"] = "nonsense"

        res = upage.request.post(VALIDATE_API_URL, data=validator_api_url)

        assert res.status == 422
        assert res.json() == {
            "detail": {"json": {"url": ["Not a valid URL."]}},
            "message": "Validation error",
        }

    def test_api_validate_by_json(self, upage, validator_api_json):
        res = upage.request.post(VALIDATE_API_URL, data=validator_api_json)

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

    def test_unversioned_path_redirects(self, upage, validator_api_json):
        res = upage.request.post(
            f"{PROXY_ORIGIN}/api/validate",
            data=validator_api_json,
            max_redirects=0,
        )

        assert res.status == 308
        assert res.headers["location"] == "/api/v1/validate"

    def test_docs_are_served_through_the_proxy(self, unauthed_page):
        unauthed_page.goto(f"{PROXY_ORIGIN}/validator/docs")

        expect(unauthed_page.get_by_role("heading", level=1)).to_contain_text(
            "Datagov Validator"
        )
        # /validate and /validate/document
        expect(unauthed_page.locator(".opblock-post")).to_have_count(2)
        # "Try it out" must go through the proxy, like every other caller
        expect(unauthed_page.locator(".servers select")).to_have_value(PROXY_ORIGIN)


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
                '{"error": "Validation of this URL\'s domain is not allowed."}',
                "Validation of this URL's domain is not allowed.",
            ),
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
                "Submission is too large. Maximum size is 150 MB.",
            ),
            (
                503,
                "application/json",
                '{"error": "The validator is busy with another large catalog. '
                'Please try again in a minute."}',
                "The validator is busy with another large catalog. "
                "Please try again in a minute.",
            ),
            (503, "text/html", "<html>Service Unavailable</html>", UNAVAILABLE),
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

    def test_incomplete_validation_is_explained(self, upage):
        upage.route(
            "**/api/v1/validate",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body='{"validation_errors": [[0, "first error"]], '
                '"validation_incomplete": true}',
            ),
        )
        self._submit_url(upage)

        expect(upage.locator("#validator-results")).to_contain_text(
            "Validation stopped early."
        )
        expect(upage.locator("#validator-results")).to_contain_text(
            "Fix the reported errors and validate again"
        )

    def test_fetch_method_is_locked_while_validating(self, upage):
        pending = []
        upage.route("**/api/v1/validate", lambda route: pending.append(route))
        self._submit_url(upage)

        expect(upage.locator("select[name=fetch_method]")).to_be_disabled()
        expect(upage.locator("select[name=schema]")).to_be_disabled()
        expect(upage.locator("input[name=url]")).to_be_disabled()
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
        expect(upage.locator("select[name=schema]")).to_be_enabled()
        expect(upage.locator("input[name=url]")).to_be_enabled()

    def test_csv_download_neutralizes_spreadsheet_formulas(self, upage):
        upage.route(
            "**/api/v1/validate",
            lambda route: route.fulfill(
                status=200,
                content_type="application/json",
                body='{"validation_errors": ['
                '["=HYPERLINK(\\"https://example.com\\")", "@SUM(1,1)"]]}',
            ),
        )
        self._submit_url(upage)

        with upage.expect_download() as download_info:
            upage.locator("#btn-download").click()

        content = download_info.value.path().read_text(encoding="utf-8")
        assert "\"'=HYPERLINK(" in content
        assert '"\'@SUM(1,1)"' in content


# Real-size submissions end to end: the browser and the HTTP request the
# /validate/ page makes, through the local nginx proxy (nginx-local-proxy.conf)
# to the validator container (GSA/data.gov#6460). Catalogs are generated at
# test time under tmp_path and deleted before each test returns.
# tests/integration/validator_api/test_large_uploads.py covers the same sizes
# against the app directly, including URL fetches.


MIB = 1024 * 1024
# nginx's own client_max_body_size for the validate location.
PROXY_BODY_LIMIT = 226 * MIB


@pytest.fixture(autouse=True)
def release_memory():
    yield
    gc.collect()


@pytest.fixture
def catalog_file(tmp_path):
    written = []

    def make(size):
        path = tmp_path / f"catalog-{len(written)}.json"
        written.append(path)
        write_catalog(path, size)
        return path

    yield make
    for path in written:
        path.unlink(missing_ok=True)


def _post_pasted(path):
    """The request the page sends for a pasted or uploaded catalog."""
    body = json.dumps(
        {"schema": FEDERAL, "fetch_method": "paste", "json_text": path.read_text()}
    ).encode()
    path.unlink()
    assert len(body) < PROXY_BODY_LIMIT
    return requests.post(
        VALIDATE_API_URL,
        data=body,
        headers={"Content-Type": "application/json"},
        timeout=120,
    )


class TestLargeSubmissionsThroughTheProxy:
    def test_a_valid_catalog_at_the_limit_is_validated(self, catalog_file):
        res = _post_pasted(catalog_file(MAX_UPLOAD_BYTES))

        assert res.status_code == 200
        assert res.json() == {"validation_errors": []}

    @pytest.mark.parametrize("size", [151 * MIB, 200 * MIB], ids=["151MB", "200MB"])
    def test_a_catalog_over_the_limit_gets_the_validators_json_refusal(
        self, catalog_file, size
    ):
        """Not nginx's bare 413: the body is within the proxy's limit, so the
        validator answers, with its own message."""
        res = _post_pasted(catalog_file(size))

        assert res.status_code == 400
        assert res.headers["Content-Type"].startswith("application/json")
        assert res.json() == {"error": PAYLOAD_TOO_LARGE_MESSAGE}


class TestCatalogAsTheBodyThroughTheProxy:
    """/validate/document, how the page sends catalogs."""

    @staticmethod
    def _post(path):
        with open(path, "rb") as body:
            res = requests.post(
                f"{VALIDATE_API_URL}/document",
                params={"schema": FEDERAL, "fetch_method": "upload"},
                data=body,
                headers={"Content-Type": "application/json"},
                timeout=120,
            )
        path.unlink()
        return res

    def test_a_valid_catalog_at_the_limit_is_validated(self, catalog_file):
        res = self._post(catalog_file(MAX_UPLOAD_BYTES))

        assert res.status_code == 200
        assert res.json() == {"validation_errors": []}

    def test_a_catalog_over_the_limit_gets_the_validators_json_refusal(
        self, catalog_file
    ):
        res = self._post(catalog_file(200 * MIB))

        assert res.status_code == 400
        assert res.headers["Content-Type"].startswith("application/json")
        assert res.json() == {"error": PAYLOAD_TOO_LARGE_MESSAGE}


class TestLargeUploadsInTheBrowser:
    @pytest.fixture
    def upload_page(self, unauthed_page):
        unauthed_page.goto(VALIDATOR_PROXY_URL)
        unauthed_page.locator("select[name=fetch_method]").select_option("upload")
        return unauthed_page

    def test_a_valid_catalog_at_the_limit_is_uploaded_and_validated(
        self, upload_page, catalog_file
    ):
        path = catalog_file(MAX_UPLOAD_BYTES)
        upload_page.locator("input[type=file][name=json_file]").set_input_files(
            str(path)
        )
        upload_page.locator("input[type=submit]").click(timeout=60000)

        expect(upload_page.locator("#validator-results")).to_contain_text(
            "No validation errors found", timeout=120000
        )

    def test_a_file_over_the_limit_is_refused_before_uploading(
        self, upload_page, catalog_file
    ):
        path = catalog_file(MAX_UPLOAD_BYTES + 1)
        upload_page.locator("input[type=file][name=json_file]").set_input_files(
            str(path)
        )
        upload_page.locator("input[type=submit]").click()

        expect(upload_page.locator("#upload_field .usa-error-message")).to_have_text(
            "File is too large. Maximum size is 150 MB."
        )
