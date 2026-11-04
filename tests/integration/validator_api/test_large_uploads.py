"""Real-size submissions to the validator API (GSA/data.gov#6460): catalogs
generated at test time at, just over and well over MAX_UPLOAD_MB, sent through
the real app with nothing patched but the private-address check. Pasted
catalogs go in the same request body the /validate/ page sends (it sends an
uploaded file the same way). URL fetches download from a local HTTP server
over a real socket.

Each test writes its catalog under tmp_path and deletes it before returning.
They take a few seconds each and peak at about 2GB in the test process, so
they're marked `large_upload`: `pytest -m "not large_upload"` skips them.
"""

import gc
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from dcatus_validation.fetch import PAYLOAD_TOO_LARGE_MESSAGE
from dcatus_validation.limits import MAX_REQUEST_BYTES, MAX_UPLOAD_BYTES, MAX_UPLOAD_MB
from tests.utils.large_catalog import (
    FEDERAL,
    INVALID_ACCESS_LEVEL,
    write_catalog,
)

pytestmark = pytest.mark.large_upload

URL = "/api/v1/validate"
MIB = 1024 * 1024
SUBMISSION_TOO_LARGE_MESSAGE = (
    f"Submission too large - must be {MAX_UPLOAD_MB}MB or less."
)


@pytest.fixture(autouse=True)
def release_memory():
    """Free each test's ~2GB before the next one: the app and test client a
    test builds are only freed by the cycle collector."""
    yield
    gc.collect()


@pytest.fixture
def catalog_file(tmp_path):
    """Write a catalog of exactly `size` bytes; every one is deleted at teardown."""
    written = []

    def make(size, **kwargs):
        path = tmp_path / f"catalog-{len(written)}.json"
        written.append(path)
        write_catalog(path, size, **kwargs)
        return path

    yield make
    for path in written:
        path.unlink(missing_ok=True)


def _paste_body(path) -> bytes:
    """What the /validate/ page sends: JSON.stringify({schema, fetch_method,
    json_text}), with the catalog JSON-escaped inside json_text."""
    return json.dumps(
        {"schema": FEDERAL, "fetch_method": "paste", "json_text": path.read_text()}
    ).encode()


def _post(client, body: bytes):
    return client.post(URL, data=body, content_type="application/json")


class _CatalogServer(BaseHTTPRequestHandler):
    """Serves one file. `?no-length` leaves out Content-Length, so the body
    ends when the connection closes and only the streamed count can stop it."""

    path_to_serve = None

    def do_GET(self):
        size = self.path_to_serve.stat().st_size
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        if not self.path.endswith("?no-length"):
            self.send_header("Content-Length", str(size))
        self.end_headers()
        try:
            with open(self.path_to_serve, "rb") as fp:
                while chunk := fp.read(MIB):
                    self.wfile.write(chunk)
        except (BrokenPipeError, ConnectionResetError):
            pass  # the validator stopped reading at the limit

    def log_message(self, *args):
        pass


@pytest.fixture
def serve():
    """Serve a file on 127.0.0.1 and return its URL."""
    servers = []

    def start(path):
        handler = type("Handler", (_CatalogServer,), {"path_to_serve": path})
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        server.daemon_threads = True
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return f"http://127.0.0.1:{server.server_port}/catalog.json"

    yield start
    for server in servers:
        server.shutdown()
        server.server_close()


class TestPastedCatalog:
    def test_a_valid_catalog_at_the_limit_is_validated(
        self, validator_client, catalog_file
    ):
        path = catalog_file(MAX_UPLOAD_BYTES)
        body = _paste_body(path)
        path.unlink()

        res = _post(validator_client, body)

        assert res.status_code == 200
        assert res.get_json() == {"validation_errors": []}

    def test_every_record_is_validated(self, validator_client, catalog_file):
        """The one bad record, last in a catalog at the limit, is reported."""
        path = catalog_file(MAX_UPLOAD_BYTES, invalid_last="last-record")
        body = _paste_body(path)
        path.unlink()

        res = _post(validator_client, body)

        assert res.status_code == 200
        [[identifier, message]] = res.get_json()["validation_errors"]
        assert identifier == "last-record"
        assert INVALID_ACCESS_LEVEL in message

    @pytest.mark.parametrize("size", [151 * MIB, 200 * MIB], ids=["151MB", "200MB"])
    def test_a_catalog_over_the_limit_is_refused(
        self, validator_client, catalog_file, size
    ):
        """Its request body fits MAX_REQUEST_BYTES, so the validator itself
        refuses the document, with its JSON message."""
        path = catalog_file(size)
        body = _paste_body(path)
        path.unlink()
        assert len(body) < MAX_REQUEST_BYTES

        res = _post(validator_client, body)

        assert res.status_code == 400
        assert res.get_json() == {"error": PAYLOAD_TOO_LARGE_MESSAGE}

    def test_a_request_body_over_the_request_limit_gets_a_json_413(
        self, validator_client, catalog_file
    ):
        path = catalog_file(230 * MIB)
        body = _paste_body(path)
        path.unlink()
        assert len(body) > MAX_REQUEST_BYTES

        res = _post(validator_client, body)

        assert res.status_code == 413
        assert res.get_json() == {"error": SUBMISSION_TOO_LARGE_MESSAGE}


class TestFetchedCatalog:
    def test_a_catalog_at_the_limit_is_fetched_and_every_record_validated(
        self, validator_client, catalog_file, serve
    ):
        url = serve(catalog_file(MAX_UPLOAD_BYTES, invalid_last="last-record"))

        res = validator_client.post(
            URL, json={"schema": FEDERAL, "fetch_method": "url", "url": url}
        )

        assert res.status_code == 200
        [[identifier, message]] = res.get_json()["validation_errors"]
        assert identifier == "last-record"
        assert INVALID_ACCESS_LEVEL in message

    @pytest.mark.parametrize(
        "size, query",
        [
            (151 * MIB, ""),
            (200 * MIB, ""),
            (151 * MIB, "?no-length"),
        ],
        ids=["151MB", "200MB", "151MB-without-content-length"],
    )
    def test_a_catalog_over_the_limit_is_refused(
        self, validator_client, catalog_file, serve, size, query
    ):
        """Refused by Content-Length before the download, or by the running
        byte count when the server doesn't send one."""
        url = serve(catalog_file(size)) + query

        res = validator_client.post(
            URL, json={"schema": FEDERAL, "fetch_method": "url", "url": url}
        )

        assert res.status_code == 400
        assert res.get_json() == {"error": PAYLOAD_TOO_LARGE_MESSAGE}
