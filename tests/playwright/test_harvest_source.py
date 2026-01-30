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
            upage.locator("table#harvest-source-config-properties tr th")
        ).to_have_text(
            [
                "Field",
                "Value",
                "Organization",
                "name",
                "url",
                "frequency",
                "schema_type",
                "source_type",
                "notification_frequency",
                "id",
            ]
        )
        expect(
            upage.locator("table#harvest-source-config-properties tr td")
        ).to_have_text(
            [
                "Test Org",
                "Test Source",
                "http://localhost:80/dcatus/dcatus.json",
                "daily",
                "dcatus1.1: federal",
                "document",
                "always",
                "2f2652de-91df-4c63-8b53-bfced20b276b",
            ]
        )

    def test_cant_perform_actions(self, upage):
        expect(
            upage.locator(".harvest-source-config-actions ul li button")
        ).to_have_count(0)

    def test_config_table_summary(self, upage):
        # Test static content in the summary table
        summary_table = upage.locator("table#harvest-source-config-summary")

        # Test static labels and values
        expect(summary_table).to_contain_text("Records")
        expect(summary_table).to_contain_text("2")
        expect(summary_table).to_contain_text("Synced Records")
        expect(summary_table).to_contain_text("Last Job Records in Error")
        expect(summary_table).to_contain_text("N/A")
        expect(summary_table).to_contain_text("Last Job Finished")
        expect(summary_table).to_contain_text("Next Job Scheduled")

    def test_job_summary(self, upage):
        expect(upage.locator("#paginated__harvest-jobs table tbody tr")).to_have_count(
            10
        )

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

    @pytest.mark.parametrize(
        "data_term_name, glossary_term_name",
        [
            ("frequency", "frequency"),
            ("schema_type", "schema_type"),
            ("source_type", "source_type"),
            ("notification_frequency", "notification_frequency"),
            ("synced records", "Synced Records"),
        ],
    )
    def test_glossary_terms(self, upage, data_term_name, glossary_term_name):
        glossary = upage.locator("#glossary")
        # glossary starts closed
        assert glossary.get_attribute("aria-hidden") == "true"

        upage.click(f"span[data-term='{data_term_name}']")
        assert glossary.get_attribute("aria-hidden") == "false"
        glossary_elem = upage.locator(
            "button[class='data-glossary-term glossary__term']"
        )
        # only 1 term (the clicked one) is present in the glossary
        expect(glossary_elem).to_have_count(1)
        assert glossary_elem.text_content() == glossary_term_name

        # close the glossary
        glossary_close = upage.get_by_title("Close glossary")
        glossary_close.click()
        assert glossary.get_attribute("aria-hidden") == "true"


class TestHarvestSourceAuthed:
    def test_can_perform_actions(self, apage):
        expect(
            apage.locator("form ul li input")
        ).to_have_text(["Edit", "Harvest", "Clear", "Delete"])

    def test_cant_delete_harvest_source_with_records(self, apage):
        apage.once("dialog", lambda dialog: dialog.accept())
        apage.get_by_role("button", name="Delete", exact=True).click()
        expect(apage.locator(".usa-alert--warning")).to_contain_text(
            ["Failed: 2 records in the Harvest source, please clear it first."]
        )

    def test_contains_notification_emails(self, apage):
        table_locator = apage.locator("table#harvest-source-config-properties")
        expect(table_locator).to_contain_text("Notification emails")
        expect(table_locator).to_contain_text("email@example.com")

    def test_contains_source_url(self, apage):
        """
        Test that the harvest source URL is a link and has the `target="_blank"`
        attribute to open in a new tab.
        """
        # Locate the anchor tag by its href attribute
        source_link = apage.locator('a[href="http://localhost:80/dcatus/dcatus.json"]')

        # Verify the link exists and has target="_blank"
        expect(source_link).to_be_visible()
        expect(source_link).to_have_attribute("target", "_blank")
        expect(source_link).to_have_text("http://localhost:80/dcatus/dcatus.json")
