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
    # ruff: noqa: E501
    @pytest.mark.parametrize(
        "text,index",
        [
            ("Organizations", 0),
            ("Harvest Sources", 1),
            ("Metrics", 2),
            ("Logout", 4),
        ],
    )  # test is parameterized to remove the check for individual logged in user name in index 3
    def test_nav_items(self, text, index, apage):
        nav_items = apage.locator("ul.menu > li")
        assert nav_items.nth(index).inner_text() == text
