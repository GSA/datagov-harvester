"""Real-size submissions end to end: the browser and the HTTP request the
/validate/ page makes, through the local nginx proxy (nginx-local-proxy.conf)
to the validator container (GSA/data.gov#6460). Catalogs are generated at
test time under tmp_path and deleted before each test returns.

tests/integration/validator_api/test_large_uploads.py covers the same sizes
against the app directly, including URL fetches.
"""

import gc
import json
import os

import pytest
import requests
from playwright.sync_api import expect

from dcatus_validation.fetch import PAYLOAD_TOO_LARGE_MESSAGE
from dcatus_validation.limits import MAX_UPLOAD_BYTES
from tests.utils.large_catalog import FEDERAL, write_catalog

pytestmark = pytest.mark.large_upload

# As in test_validator.py: only the local proxy routes /api/v1/validate.
PROXY_ORIGIN = f"http://localhost:{os.getenv('PROXY_PORT', '8082')}"
VALIDATOR_PROXY_URL = f"{PROXY_ORIGIN}/validate/"
VALIDATE_API_URL = f"{PROXY_ORIGIN}/api/v1/validate"
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
