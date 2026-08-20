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
                "Name",
                "Test Org",
                "Logo",
                "https://raw.githubusercontent.com/GSA/datagov-harvester/refs/heads/main/app/static/assets/img/placeholder-organization.png",
                "Description",
                "Fixture org description",
                "Slug",
                "fixture-org",
                "Organization type",
                "Federal Government",
                "Code repo URL",
                "None",
                "Code repo exempt",
                "No",
                "Aliases",
                "testorg",
                "ID",
                "d925f84d-955b-4cb7-812f-dcfd6681a18f",
                "Source count",
                "2",
            ]
        )

    def test_harvest_source_table(self, upage):
        expect(
            upage.locator(".organization-harvest-source-list table.usa-table tbody tr")
        ).to_have_count(2)
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
                "Test ISO19115 source",
                "\n",  # last job status icon
                "N/A",
                "waf",
                "daily",
                "http://localhost:80/waf",
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
        expect(apage.locator(".usa-alert--warning")).to_contain_text(
            [
                "Failed: 2 harvest sources in the organization, please delete those first."
            ]
        )

    def test_contains_notification_emails(self, apage):
        expect(
            apage.locator(".organization-harvest-source-list table.usa-table thead tr")
        ).to_contain_text("Notification Emails")
        expect(
            apage.locator(".organization-harvest-source-list table.usa-table tbody tr")
            .filter(has_text="Test Source")
            .first
        ).to_contain_text("email@example.com")


class TestOrganizationCodeRepoFields:
    """Test code repository URL and exemption fields in organization forms."""

    def test_add_form_has_code_repo_fields(self, authed_page):
        """Test that add organization form includes code repo fields."""
        authed_page.goto("/organization/add")

        # Check code_repo_url field exists
        code_repo_url_field = authed_page.locator("input#code_repo_url")
        expect(code_repo_url_field).to_be_visible()

        # Check code_repo_exempt checkbox exists
        code_repo_exempt_field = authed_page.locator("input#code_repo_exempt")
        expect(code_repo_exempt_field).to_be_visible()
        expect(code_repo_exempt_field).to_have_attribute("type", "checkbox")

        # Check label exists
        exempt_label = authed_page.locator('label[for="code_repo_exempt"]')
        expect(exempt_label).to_contain_text("OMB-approved exemption")

    def test_add_organization_with_invalid_url(self, authed_page):
        """Test that invalid URL protocol shows error."""
        authed_page.goto("/organization/add")

        # Fill in required fields
        authed_page.locator("input#name").fill("Test Org Invalid URL")
        authed_page.locator("input#slug").fill("test-org-invalid-url")
        authed_page.locator("input#logo").fill("https://example.com/logo.png")
        authed_page.locator("input#code_repo_url").fill("ftp://github.com/test")

        # Submit form
        authed_page.get_by_role("button", name="Submit").click()

        # Should stay on form with error message
        expect(authed_page).to_have_url("/organization/add")
        expect(authed_page.locator(".usa-alert--error")).to_contain_text(
            "URL must start with http"
        )

        # Form fields should retain values
        expect(authed_page.locator("input#name")).to_have_value("Test Org Invalid URL")
        expect(authed_page.locator("input#slug")).to_have_value("test-org-invalid-url")

    def test_add_organization_with_conflict_shows_warning(self, authed_page):
        """Test that setting both URL and exempt flag shows warning but allows submission."""
        authed_page.goto("/organization/add")

        # Fill in required fields
        authed_page.locator("input#name").fill("Test Org Conflict")
        authed_page.locator("input#slug").fill("test-org-conflict")
        authed_page.locator("input#logo").fill("https://example.com/logo.png")
        authed_page.locator("input#code_repo_url").fill("https://github.com/test")

        # Check the exempt checkbox - use evaluate to click it directly
        authed_page.locator("input#code_repo_exempt").evaluate("el => el.click()")

        # Submit form
        authed_page.get_by_role("button", name="Submit").click()

        # Should redirect to organization list (form submitted successfully)
        expect(authed_page).to_have_url("/organization_list/")

        # Should show a warning flash message
        expect(
            authed_page.locator(".usa-alert--warning").filter(
                has_text="both a repository URL and an exemption"
            )
        ).to_be_visible()

    def test_edit_form_has_code_repo_fields(self, apage):
        """Test that edit form has code repo fields."""
        # Navigate directly to edit page for fixture org
        apage.goto("/organization/edit/d925f84d-955b-4cb7-812f-dcfd6681a18f")

        # Check that code repo fields exist in the form
        expect(apage.locator("input#code_repo_url")).to_be_visible()
        expect(apage.locator("input#code_repo_exempt")).to_be_visible()

    def test_detail_page_displays_code_repo_fields(self, upage):
        """Test that organization detail page shows code repo fields."""
        # Verify the config table includes Code repo URL and Code repo exempt fields
        expect(upage.locator(".organization-config-properties table")).to_contain_text(
            "Code repo URL"
        )
        expect(upage.locator(".organization-config-properties table")).to_contain_text(
            "Code repo exempt"
        )
