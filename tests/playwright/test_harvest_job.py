import csv

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


class TestHarvestJobUnauthed:
    def test_config_table_properties(self, upage):
        # Test specific static labels and values that don't change
        table = upage.locator(".harvest-job-config-properties table")

        # Test static content that should always be present
        expect(table).not_to_contain_text("Percent complete:")  # job is not in progress
        expect(table).to_contain_text("Harvest Source:")
        expect(table).to_contain_text("Test Source")
        expect(table).to_contain_text("status:")
        expect(table).to_contain_text("error")  # From fixtures
        expect(table).to_contain_text("job_type:")
        expect(table).to_contain_text("harvest")
        expect(table).to_contain_text("records_total:")
        expect(table).to_contain_text("10")
        expect(table).to_contain_text("records_added:")
        expect(table).to_contain_text("2")
        expect(table).to_contain_text("records_updated:")
        expect(table).to_contain_text("0")
        expect(table).to_contain_text("records_deleted:")
        expect(table).to_contain_text("records_errored:")
        expect(table).to_contain_text("8")
        expect(table).to_contain_text("records_unchanged:")
        expect(table).to_contain_text("records_validated:")
        expect(table).to_contain_text("id:")
        expect(table).to_contain_text("6bce761c-7a39-41c1-ac73-94234c139c76")

        # Test that date fields exist but don't check exact values
        expect(table).to_contain_text("date_created:")
        expect(table).to_contain_text("date_finished:")

    def test_harvest_job_record_errors_display(self, upage):
        expect(
            upage.locator("#error_results_pagination .error-list .error-block")
        ).to_have_count(
            10
        )  # paginated at 10 entries

        expect(
            upage.locator(
                "#error_results_pagination .error-list .error-block:first-child p a"
            )
        ).to_have_attribute(
            "href",
            "/harvest_record/0779c855-df20-49c8-9108-66359d82b77c",
        )

    def test_harvest_job_record_errors_summary(self, upage):
        expect(upage.locator("table#harvest-job-error-summary")).to_be_visible()

        expect(upage.locator("table#harvest-job-error-summary thead tr")).to_have_count(
            1
        )
        expect(
            upage.locator("table#harvest-job-error-summary tbody tr td")
        ).to_have_text(["TestException", "8", "ValidationException", "8"])

    def test_download_harvest_errors_csv(self, upage):
        pytest_harvest_errors_csv = "pytest_harvest_errors.csv"
        with upage.expect_download() as download_info:
            upage.get_by_text("download record errors as .csv").click()
        download = download_info.value
        download.save_as(pytest_harvest_errors_csv)
        with open(pytest_harvest_errors_csv) as csvfile:
            data = csv.reader(csvfile)
            # assert row count
            assert 17 == sum(1 for row in data)
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

    @pytest.mark.parametrize(
        "data_term_name, glossary_term_name",
        [
            ("records_unchanged", "records_unchanged"),
            ("job error", "Job Error"),
            ("record error", "Record Error"),
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


class TestHarvestJobAuthed:
    pass
