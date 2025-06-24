import pytest

from playwright.sync_api import expect


@pytest.fixture()
def upage(unauthed_page):
    unauthed_page.goto("/metrics/")
    yield unauthed_page


class TestMetricsUnauthed:

    def test_new_jobs(self, upage):
        expect(upage.locator("#jobs-to-harvest")).to_contain_text(
            "scheduled jobs still to harvest:", ignore_case=True
        )
