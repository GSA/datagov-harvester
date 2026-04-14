import os
from urllib.parse import urlparse

import pytest
from dotenv import load_dotenv
from flask import Flask
from flask.sessions import SecureCookieSessionInterface
from playwright.sync_api import sync_playwright

load_dotenv()


def _build_signed_session_cookie() -> str:
    """Build a Flask session cookie for authenticated Playwright pages."""
    secret = os.getenv("FLASK_APP_SECRET_KEY")
    if not secret:
        raise RuntimeError("FLASK_APP_SECRET_KEY is required for Playwright auth")

    app = Flask(__name__)
    app.secret_key = secret
    serializer = SecureCookieSessionInterface().get_signing_serializer(app)
    if serializer is None:
        raise RuntimeError("Unable to create Flask session serializer")

    session_user = os.getenv("PLAYWRIGHT_AUTH_USER", "user@test.local")
    return serializer.dumps({"user": session_user})


def _authed_storage_state(base_url: str) -> dict:
    host = urlparse(base_url).hostname or "localhost"
    return {
        "cookies": [
            {
                "name": "session",
                "value": _build_signed_session_cookie(),
                "domain": host,
                "path": "/",
                "expires": -1,
                "httpOnly": True,
                "secure": False,
                "sameSite": "Lax",
            }
        ],
        "origins": [],
    }


@pytest.fixture(scope="session")
def base_url():
    return "http://localhost:8080/"


@pytest.fixture(scope="session")
def browser():
    # Start Playwright
    playwright = sync_playwright().start()
    # Launch a browser (e.g., Chromium)
    browser = playwright.chromium.launch(headless=True, channel="chrome")
    yield browser
    # Close the browser after tests are done
    browser.close()


@pytest.fixture()
def unauthed_page(browser, base_url):
    context = browser.new_context(base_url=base_url)
    page = context.new_page()
    page.set_default_timeout(2500)
    yield page
    context.close()


@pytest.fixture()
def authed_page(browser, base_url):
    context = browser.new_context(
        base_url=base_url, storage_state=_authed_storage_state(base_url)
    )
    page = context.new_page()
    page.set_default_timeout(2500)
    yield page
    context.close()


@pytest.fixture
def dcatus_many_invalid_json():
    """
    A DCAT-US catalog with 15 records all missing 'identifier',
    producing >10 validation errors so the download button renders.
    """
    import json

    datasets = [
        {
            "title": f"Dataset {i}",
            "description": f"Description {i}",
            "accessLevel": "public",
            "bureauCode": ["000:00"],
            "programCode": ["000:000"],
        }
        for i in range(15)
    ]
    return json.dumps({"dataset": datasets})
