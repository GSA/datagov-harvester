import pytest
from playwright.sync_api import expect


@pytest.fixture()
def upage(unauthed_page):
    unauthed_page.goto("/harvest_source/2f2652de-91df-4c63-8b53-bfced20b276b")
    yield unauthed_page


@pytest.fixture()
def apage(authed_page):
    authed_page.goto("/harvest_source/2f2652de-91df-4c63-8b53-bfced20b276b")
    yield authed_page


class TestHarvestSourceUnauthed:
    def test_config_table_properties(self, upage):
        expect(
            upage.locator(".harvest-source-config-properties table tr td")
        ).to_have_text(
            [
                "Organization:",
                "Test Org",
                "name:",
                "Test Source",
                "url:",
                "http://localhost:80/dcatus/dcatus.json",
                "Notification emails:",
                "email@example.com",
                "frequency:",
                "daily",
                "schema_type:",
                "dcatus1.1: federal",
                "source_type:",
                "document",
                "notification_frequency:",
                "always",
                "id:",
                "2f2652de-91df-4c63-8b53-bfced20b276b",
            ]
        )

    def test_cant_perform_actions(self, upage):
        expect(
            upage.locator(".harvest-source-config-actions ul li button")
        ).to_have_count(0)

    def test_config_table_summary(self, upage):
        # Test static content in the summary table
        summary_table = upage.locator(".harvest-source-config-summary table")
        
        # Test static labels and values
        expect(summary_table).to_contain_text("Records:")
        expect(summary_table).to_contain_text("2")
        expect(summary_table).to_contain_text("Synced Records:")
        expect(summary_table).to_contain_text("Last Job Records in Error:")
        expect(summary_table).to_contain_text("N/A")
        expect(summary_table).to_contain_text("Last Job Finished:")
        expect(summary_table).to_contain_text("Next Job Scheduled:")

    def test_job_summary(self, upage):
        expect(upage.locator("#paginated__harvest-jobs table tbody tr")).to_have_count(
            10
        )

        table = upage.locator(".harvest-job-config-properties table")
        
        # Test the first row of the job table, but skip the dynamic date columns
        first_row = upage.locator("#paginated__harvest-jobs table tr:first-child td")
        
        # Test static content that doesn't change
        expect(first_row.nth(0)).to_contain_text("4e5f6a")  # Job ID (truncated)
        expect(first_row.nth(1)).to_contain_text("in_progress")  # Status
        # Skip columns 2 and 3 (date_created and date_finished) as they're dynamic
        expect(first_row.nth(4)).to_contain_text("0")  # records_added
        expect(first_row.nth(5)).to_contain_text("0")  # records_updated
        expect(first_row.nth(6)).to_contain_text("0")  # records_deleted
        expect(first_row.nth(7)).to_contain_text("0")  # records_errored
        expect(first_row.nth(8)).to_contain_text("0")  # records_ignored


class TestHarvestSourceAuthed:
    def test_can_perform_actions(self, apage):
        expect(
            apage.locator(".harvest-source-config-actions ul li input")
        ).to_have_text(["Edit", "Harvest", "Clear", "Delete"])

    def test_cant_delete_harvest_source_with_records(self, apage):
        apage.once("dialog", lambda dialog: dialog.accept())
        apage.get_by_role("button", name="Delete", exact=True).click()
        expect(apage.locator(".alert-warning")).to_contain_text(
            ["Failed: 2 records in the Harvest source, please clear it first."]
        )
