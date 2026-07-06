import pytest
from playwright.sync_api import expect


@pytest.fixture()
def upage(unauthed_page):
    unauthed_page.goto("/harvest_source_list")
    yield unauthed_page


@pytest.fixture()
def apage(authed_page):
    authed_page.goto("/harvest_source_list")
    yield authed_page


class TestHarvestSourceListUnauthed:
    def test_has_correct_page_title(self, upage):
        expect(upage).to_have_title("Harvest Source List")

    def test_can_see_source_list(self, upage):
        expect(upage.locator("table.usa-table tbody tr")).to_have_count(1)
        expect(upage.locator("table.usa-table tbody tr th[scope='row']")).to_have_text(
            ["Test Source"]
        )

    def test_cant_add_source(self, upage):
        expect(upage.locator("text=Add Harvest Source")).to_have_count(0)


class TestHarvestSourceListAuthed:
    def test_can_add_source(self, apage):
        expect(apage.locator("text=Add Harvest Source")).to_have_count(1)
