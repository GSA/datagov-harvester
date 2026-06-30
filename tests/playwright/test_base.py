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
    def test_has_mobile_viewport_meta(self, upage):
        expect(upage.locator('meta[name="viewport"]')).to_have_attribute(
            "content", "width=device-width, initial-scale=1"
        )

    def test_nav_items(self, upage):
        expect(upage.locator("ul.menu > li")).to_have_text(
            [
                "Organizations",
                "Harvest Sources",
                "API Documentation",
                "Metrics",
                "Validators",
                "Glossary\nLogin",
            ]
        )


class TestBaseAuthed:
    def test_nav_items(self, apage):
        nav_items_expected = [
            ("Organizations", 0),
            ("Harvest Sources", 1),
            ("API Documentation", 2),
            ("Metrics", 3),
            ("Validators", 4),
        ]
        nav_items = apage.locator("ul.menu > li")
        for text, index in nav_items_expected:
            assert nav_items.nth(index).inner_text() == text

        utility = nav_items.nth(5).inner_text()
        assert "Glossary" in utility
        assert "user@test.local" in utility
        assert "Logout" not in utility

        apage.locator(".js-account-menu-toggle").click()
        expect(apage.get_by_role("menuitem", name="Logout")).to_be_visible()

    def test_mobile_drawer_shows_flat_account_logout(self, apage):
        apage.set_viewport_size({"width": 375, "height": 667})
        apage.get_by_role("button", name="Menu").click()

        expect(apage.locator(".harvester-nav__account-mobile")).to_be_visible()
        expect(
            apage.locator(".harvester-nav__account-mobile .harvester-nav__account-name")
        ).to_have_text("user@test.local")
        expect(
            apage.locator(".harvester-nav__account-mobile .harvester-nav__logout-mobile")
        ).to_be_visible()
        expect(apage.locator(".js-account-menu-toggle")).to_be_hidden()

        account_box = apage.locator(".harvester-nav__account-mobile").bounding_box()
        menu_box = apage.locator("ul.menu").bounding_box()
        assert account_box is not None
        assert menu_box is not None
        assert account_box["y"] < menu_box["y"]
