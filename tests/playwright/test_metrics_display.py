import pytest
from playwright.sync_api import expect


@pytest.fixture()
def upage(
    unauthed_page, interface, organization_data, source_data_dcatus, job_data_new
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    interface.add_harvest_job(job_data_new)

    unauthed_page.goto("/metrics/")
    yield unauthed_page


class TestMetricsUnauthed:
    def test_new_jobs_exist(self, upage):
        """has scheduled jobs table"""
        expect(upage.locator("#jobs-to-harvest")).to_be_visible()

    def test_new_jobs_table(self, upage, interface):
        """scheduled jobs table has rows

        fixture data has one new job
        """
        upage.goto("/metrics/")
        jobs_section = upage.locator("#jobs-to-harvest")
        print(jobs_section.inner_html())
        expect(jobs_section.locator("tbody tr")).to_have_count(2)

    def test_failed_jobs_exist(self, upage):
        """has failed jobs section"""
        expect(upage.locator("#recent-failed-jobs")).to_be_visible()

    def test_jobs_in_progress(self, upage):
        """has "Jobs in Progress" table and progress bar checks"""
        expect(upage.locator("#jobs-harvesting")).to_be_visible()

        expect(upage.locator(".progress-meter")).to_have_count(1)
        expect(upage.locator(".progress-percent")).to_have_attribute(
            "class", "progress-percent this-progress"
        )
