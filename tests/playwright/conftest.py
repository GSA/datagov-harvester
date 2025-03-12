import pytest

from playwright.sync_api import sync_playwright, Page


@pytest.fixture(scope="session")
def base_url():
    return "http://localhost:8080/"


@pytest.fixture(scope="session")
def browser():
    # Start Playwright
    playwright = sync_playwright().start()
    # Launch a browser (e.g., Chromium)
    browser = playwright.chromium.launch(headless=True)
    yield browser
    # Close the browser after tests are done
    browser.close()


@pytest.fixture()
def unauthed_page(browser, base_url):
    context = browser.new_context(base_url=base_url)
    page = context.new_page()
    page.set_default_timeout(5000)
    yield page


@pytest.fixture()
def authed_page(browser, base_url):
    context = browser.new_context(
        base_url=base_url, storage_state="pytest_auth_state.json"
    )
    page = context.new_page()
    page.set_default_timeout(5000)
    yield page
