import pytest
from playwright.sync_api import expect


@pytest.fixture()
def upage(unauthed_page):
    unauthed_page.goto("/harvest_job/6bce761c-7a39-41c1-ac73-94234c139c76")
    yield unauthed_page


@pytest.fixture()
def apage(authed_page):
    authed_page.goto("/harvest_job/6bce761c-7a39-41c1-ac73-94234c139c76")
    yield authed_page


class TestHarvestSourceUnauthed:
    def test_config_table_properties(self, upage):
        expect(
            upage.locator(".harvest-job-config-properties table tr td")
        ).to_have_text(
            [
                "harvest_source_id:",
                "2f2652de-91df-4c63-8b53-bfced20b276b",
                "status:",
                "complete",
                "job_type:",
                "harvest",
                "date_created:",
                "2025-02-11 22:41:03.716252",
                "date_finished:",
                "2025-02-11 22:41:03.715310",
                "records_total:",
                "10",
                "records_added:",
                "2",
                "records_updated:",
                "0",
                "records_deleted:",
                "0",
                "records_errored:",
                "8",
                "records_ignored:",
                "0",
                "records_validated:",
                "0",
                "id:",
                "6bce761c-7a39-41c1-ac73-94234c139c76",
            ]
        )

    def test_harvest_job_record_errors_display(self, upage):
        expect(
            upage.locator("#error_results_pagination .error-list .error-block")
        ).to_have_count(8)

        expect(
            upage.locator(
                "#error_results_pagination .error-list .error-block:first-child p a"
            )
        ).to_have_attribute(
            "href",
            "/harvest_record/0779c855-df20-49c8-9108-66359d82b77c",
        )


class TestHarvestSourceAuthed:
    pass
