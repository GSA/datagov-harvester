import pytest
from playwright.sync_api import expect


@pytest.fixture()
def upage(unauthed_page):
    unauthed_page.goto("/organization/d925f84d-955b-4cb7-812f-dcfd6681a18f")
    yield unauthed_page


@pytest.fixture()
def apage(authed_page):
    authed_page.goto("/organization/d925f84d-955b-4cb7-812f-dcfd6681a18f")
    yield authed_page


class TestOrganizationUnauthed:
    def test_config_table_properties(self, upage):
        expect(
            upage.locator(".organization-config-properties table tr td")
        ).to_have_text(
            [
                'name:',
                'Test Org',
                'logo:',
                'https://raw.githubusercontent.com/GSA/datagov-harvester/refs/heads/main/app/static/assets/img/placeholder-organization.png',
                'description:',
                'Fixture org description',
                'slug:',
                'fixture-org',
                'organization_type:',
                'Federal Government',
                'id:',
                'd925f84d-955b-4cb7-812f-dcfd6681a18f',
            ]
        )

    def test_harvest_source_table(self, upage):
        expect(
            upage.locator(".organization-harvest-source-list table.usa-table tbody tr")
        ).to_have_count(1)
        expect(
            upage.locator(".organization-harvest-source-list table.usa-table tr td")
        ).to_have_text(
            [
                "Test Source",
                "\n",  # last job status icon
                "N/A",
                "document",
                "daily",
                "http://localhost:80/dcatus/dcatus.json",
            ]
        )
        # Locate the anchor tag by its href attribute
        source_link = upage.locator('a[href="http://localhost:80/dcatus/dcatus.json"]')

        # Verify the link exists and has target="_blank"
        expect(source_link).to_be_visible()
        expect(source_link).to_have_attribute("target", "_blank")
        expect(source_link).to_have_text("http://localhost:80/dcatus/dcatus.json")

    def test_cant_perform_actions(self, upage):
        expect(
            upage.locator(".organization-config-actions ul li button")
        ).to_have_count(0)


class TestOrganizationAuthed:
    def test_can_perform_actions(self, apage):
        expect(apage.locator(".organization-config-actions ul li input")).to_have_text(
            ["Edit", "Delete"]
        )

    def test_cant_delete_org_with_harvest_sources(self, apage):
        apage.once("dialog", lambda dialog: dialog.accept())
        apage.get_by_role("button", name="Delete", exact=True).click()
        # ruff: noqa: E501
        expect(apage.locator(".alert-warning")).to_contain_text(
            [
                "Failed: 1 harvest sources in the organization, please delete those first."
            ]
        )

    def test_contains_notification_emails(self, apage):
        expect(
            apage.locator(".organization-harvest-source-list table.usa-table thead tr")
        ).to_contain_text("Notification Emails")
        expect(
            apage.locator(".organization-harvest-source-list table.usa-table tbody tr")
        ).to_contain_text("['email@example.com'] ")
