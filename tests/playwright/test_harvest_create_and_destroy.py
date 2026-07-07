import os
from uuid import uuid4

import pytest
from playwright.sync_api import expect

api_token = os.getenv("HARVEST_API_TOKEN")


@pytest.fixture()
def apage(authed_page):
    authed_page.goto("")
    yield authed_page


@pytest.fixture()
def apage_with_org(apage):
    suffix = uuid4().hex[:8]
    org_name = f"Test Org New {suffix}"
    org_slug = f"test-org-new-{suffix}"

    apage.get_by_role("link", name="Organizations").click()
    apage.get_by_role("link", name="Add Organization").click()
    apage.get_by_role("textbox", name="Name").click()
    apage.get_by_role("textbox", name="Name").fill(org_name)
    apage.get_by_role("textbox", name="Slug").click()
    apage.get_by_role("textbox", name="Slug").fill(org_slug)
    apage.get_by_role("textbox", name="Logo").click()
    apage.get_by_role("textbox", name="Logo").fill("https://example.com/logo.png")
    apage.get_by_role("button", name="Submit").click()
    yield apage
    apage.get_by_role("link", name="Organizations").click()
    apage.get_by_role("row", name=org_name).get_by_role("link", name="Details").click()
    apage.once("dialog", lambda dialog: dialog.accept())
    apage.get_by_role("button", name="Delete", exact=True).click()


class TestHarvestCreateAndDestroy:
    def test_can_create_and_destroy_new_org(self, apage):
        suffix = uuid4().hex[:8]
        org_name = f"Test Org New {suffix}"
        org_slug = f"test-org-new-{suffix}"

        apage.get_by_role("link", name="Organizations").click()
        apage.get_by_role("link", name="Add Organization").click()
        apage.get_by_role("textbox", name="Name").click()
        apage.get_by_role("textbox", name="Name").fill(org_name)
        apage.get_by_role("textbox", name="Slug").click()
        apage.get_by_role("textbox", name="Slug").fill(org_slug)
        apage.get_by_role("textbox", name="Logo").click()
        apage.get_by_role("textbox", name="Logo").fill("https://example.com/logo.png")
        apage.get_by_role("button", name="Submit").click()
        expect(apage.locator(".usa-alert--warning")).to_contain_text(
            ["Added new organization"]
        )

        apage.get_by_role("link", name="Organizations").click()
        apage.get_by_role("row", name=org_name).get_by_role(
            "link", name="Details"
        ).click()
        apage.once("dialog", lambda dialog: dialog.accept())
        apage.get_by_role("button", name="Delete", exact=True).click()
        expect(apage.locator(".usa-alert--warning")).to_contain_text(
            ["Deleted organization with ID:"]
        )

    def test_can_create_and_destroy_new_harvest_source(self, apage_with_org):
        apage_with_org.get_by_role("link", name="Harvest Sources").click()
        apage_with_org.get_by_role("link", name="Add Harvest Source").click()
        apage_with_org.get_by_role("textbox", name="Name").click()
        apage_with_org.get_by_role("textbox", name="Name").fill("Test Source New")
        apage_with_org.get_by_role("textbox", name="Name").press("Tab")
        apage_with_org.get_by_role("textbox", name="URL").fill(
            "https://harvestsourceurl.gov/data.json"
        )
        apage_with_org.get_by_role("textbox", name="URL").press("Tab")
        apage_with_org.get_by_role("textbox", name="Notification emails").fill(
            "a@a.com"
        )
        apage_with_org.get_by_role("textbox", name="Notification emails").press("Tab")
        apage_with_org.get_by_label("Frequency", exact=True).press("Tab")
        apage_with_org.get_by_role("button", name="Submit").click()
        expect(apage_with_org.locator(".usa-alert--warning")).to_contain_text(
            ["Added new harvest source with ID:"]
        )

        apage_with_org.get_by_role("link", name="Test Source New").click()
        apage_with_org.get_by_role("button", name="Edit", exact=True).click()
        assert apage_with_org.locator("#name").get_attribute("readonly") is not None

        apage_with_org.get_by_role("link", name="Harvest Sources").click()

        apage_with_org.get_by_role("link", name="Test Source New").click()
        apage_with_org.once("dialog", lambda dialog: dialog.accept())
        apage_with_org.get_by_role("button", name="Delete", exact=True).click()
        expect(apage_with_org.locator(".usa-alert--warning")).to_contain_text(
            ["Deleted harvest source with ID:"]
        )
