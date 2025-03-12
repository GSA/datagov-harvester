import pytest
from playwright.sync_api import expect


@pytest.fixture()
def upage(unauthed_page):
    unauthed_page.goto("")
    yield unauthed_page


@pytest.fixture()
def apage(authed_page):
    authed_page.goto("")
    yield authed_page


class TestBaseUnauthed:
    def test_nav_items(self, upage):
        expect(upage.locator("ul.menu > li")).to_have_text(
            ["Organizations", "Harvest Sources", "Metrics", "Login"]
        )


class TestBaseAuthed:
    def test_nav_items(self, apage):
        nav_items_expected = [
            ("Organizations", 0),
            ("Harvest Sources", 1),
            ("Metrics", 2),
            ("Logout", 4),
        ]  # skip logged in user name in index 3
        nav_items = apage.locator("ul.menu > li")
        for text, index in nav_items_expected:
            assert nav_items.nth(index).inner_text() == text
