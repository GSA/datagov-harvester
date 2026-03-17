import pytest
from playwright.sync_api import expect


@pytest.fixture()
def page(unauthed_page):
    unauthed_page.goto("/openapi/docs")
    yield unauthed_page


class TestOpenAPI:
    def test_swagger(self, page):
        expect(page.locator("h2.title")).to_have_text("Datagov Harvester 0.1.0 OAS 3.0")
        expect(page.locator("h3")).to_have_text("Api")
        expect(page.locator(".opblock-get")).to_have_count(15)
        expect(page.locator(".model-container")).to_have_count(7)
