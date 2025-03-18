import pytest
from playwright.sync_api import expect


@pytest.fixture()
def apage(authed_page):
    authed_page.goto("")
    yield authed_page


class TestHarvestCreateAndDestroy:
    def test_can_create_and_destroy_new_org(self, apage):
        apage.get_by_role("link", name="Organizations").click()
        apage.get_by_role("link", name="Add Organization").click()
        apage.get_by_role("textbox", name="Name").click()
        apage.get_by_role("textbox", name="Name").fill("Test Org New")
        apage.get_by_role("textbox", name="Logo").click()
        apage.get_by_role("textbox", name="Logo").fill("https://example.com/logo.png")
        apage.get_by_role("button", name="Submit").click()
        expect(apage.locator(".alert-warning")).to_contain_text(
            ["Added new organization"]
        )

        apage.get_by_role("listitem").filter(
            has_text="Test Org New Details"
        ).get_by_role("link").click()
        apage.once("dialog", lambda dialog: dialog.accept())
        apage.get_by_role("button", name="Delete", exact=True).click()
        expect(apage.locator(".alert-warning")).to_contain_text(
            ["Organization deleted successfully"]
        )

    def test_can_create_and_destroy_new_harvest_source(self, apage):
        apage.get_by_role("link", name="Harvest Sources").click()
        apage.get_by_role("link", name="Add Harvest Source").click()
        apage.get_by_role("textbox", name="Name").click()
        apage.get_by_role("textbox", name="Name").fill("Test Source New")
        apage.get_by_role("textbox", name="Name").press("Tab")
        apage.get_by_role("textbox", name="URL").fill(
            "https://harvestsourceurl.gov/data.json"
        )
        apage.get_by_role("textbox", name="URL").press("Tab")
        apage.get_by_role("textbox", name="Notification_emails").fill("a@a.com")
        apage.get_by_role("textbox", name="Notification_emails").press("Tab")
        apage.get_by_label("Frequency", exact=True).press("Tab")
        apage.get_by_role("button", name="Submit").click()
        expect(apage.locator(".alert-warning")).to_contain_text(
            ["Added new harvest source"]
        )

        apage.get_by_role("listitem").filter(
            has_text="Test Source New Details"
        ).get_by_role("link").click()
        apage.once("dialog", lambda dialog: dialog.accept())
        apage.get_by_role("button", name="Delete", exact=True).click()
        expect(apage.locator(".alert-warning")).to_contain_text(
            ["Harvest source deleted successfully"]
        )
