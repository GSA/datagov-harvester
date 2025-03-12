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


class TestOrganizationListUnauthed:
    def test_has_correct_page_title(self, upage):
        expect(upage).to_have_title("Organization List")

    def test_can_see_org_list(self, upage):
        expect(upage.locator("ul.usa-card-group li.usa-card")).to_have_count(1)
        expect(
            upage.locator("ul.usa-card-group li.usa-card .usa-card__heading")
        ).to_have_text(["Test Org"])

    def test_cant_add_org(self, upage):
        expect(upage.locator(f"text=Add Organization")).to_have_count(0)


class TestOrganizationListAuthed:
    def test_can_add_org(self, apage):
        expect(apage.locator(f"text=Add Organization")).to_have_count(1)
