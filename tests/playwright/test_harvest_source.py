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
                "organization_id:",
                "d925f84d-955b-4cb7-812f-dcfd6681a18f",
                "name:",
                "Test Source",
                "url:",
                "http://localhost:80/dcatus/dcatus.json",
                "notification_emails:",
                "['email@example.com']",
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
        expect(
            upage.locator(".harvest-source-config-summary table tr td")
        ).to_have_text(
            [
                "Records:",
                "2",
                "Synced Records:",
                "2",
                "Last Job Records in Error:",
                "N/A",
                "Last Job Finished:",
                "N/A",
                "Next Job Scheduled:",
                "N/A",
            ]
        )

    def test_job_summary(self, upage):
        expect(upage.locator("#paginated__harvest-jobs table tbody tr")).to_have_count(
            2
        )
        expect(
            upage.locator("#paginated__harvest-jobs table tr:first-child td")
        ).to_have_text(
            ["harvest", "new", "2025-02-27 00:00:00", "N/A", "0", "0", "0", "0", "0"]
        )


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
