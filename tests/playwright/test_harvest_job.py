import csv
import re

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
                "Harvest Source:",
                "Test Source",
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

    def test_download_harvest_errors_csv(self, upage):
        pytest_harvest_errors_csv = "pytest_harvest_errors.csv"
        with upage.expect_download() as download_info:
            upage.get_by_text("download record errors as .csv").click()
        download = download_info.value
        download.save_as(pytest_harvest_errors_csv)
        with open(pytest_harvest_errors_csv) as csvfile:
            data = csv.reader(csvfile)
            # assert row count
            assert 9 == sum(1 for row in data)
            for index, row in enumerate(data):
                # assert headers
                if index == 0:
                    assert row == [
                        "record_error_id",
                        "identifier",
                        "title",
                        "harvest_record_id",
                        "record_error_type",
                        "message",
                        "date_created",
                    ]
                # assert contents of first row
                if index == 1:
                    expected = [
                        "04728898-237d-483a-be8c-b395bb21d199",
                        "test_identifier-1",
                        "test-0",
                        "0779c855-df20-49c8-9108-66359d82b77c",
                        "ValidationException",
                        "record is invalid",
                        "2025-03-12 16:55:01.716308",
                    ]
                    for index, item in enumerate(row):
                        if index == 6:
                            pass  # avoid date_created which is dynamic
                        else:
                            assert item == expected[index]
        # clean up test resources
        download.delete()


class TestHarvestSourceAuthed:
    pass
