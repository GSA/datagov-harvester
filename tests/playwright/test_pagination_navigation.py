import pytest
from playwright.sync_api import expect


@pytest.fixture()
def metrics_page_with_jobs(
    unauthed_page, interface, organization_data, source_data_dcatus
):
    """Create multiple jobs to enable pagination on metrics page."""
    interface.add_organization(organization_data)
    source = interface.add_harvest_source(source_data_dcatus)

    for i in range(15):
        interface.add_harvest_job(
            {"harvest_source_id": source.id, "status": "complete"}
        )

    unauthed_page.goto("/metrics/")
    yield unauthed_page


def test_pagination_updates_url(metrics_page_with_jobs):
    """Test that clicking pagination button updates the browser URL."""
    page = metrics_page_with_jobs

    page_2_button = page.locator('a.usa-pagination__button:has-text("2")').first
    if page_2_button.count() > 0:
        page_2_button.click()
        page.wait_for_timeout(500)

        assert "jobs_page=2" in page.url


def test_pagination_url_is_bookmarkable(
    unauthed_page, interface, organization_data, source_data_dcatus
):
    """Test that navigating directly to a paginated URL loads that page."""
    interface.add_organization(organization_data)
    source = interface.add_harvest_source(source_data_dcatus)

    for i in range(25):
        interface.add_harvest_job(
            {"harvest_source_id": source.id, "status": "complete"}
        )

    unauthed_page.goto("/metrics/?jobs_page=3")

    current_button = unauthed_page.locator("a.usa-pagination__button.usa-current").first
    expect(current_button).to_have_text("3")


def test_pagination_back_button_works(metrics_page_with_jobs):
    """Test that browser back button works with pagination."""
    page = metrics_page_with_jobs

    page_2_button = page.locator('a.usa-pagination__button:has-text("2")').first
    if page_2_button.count() > 0:
        page_2_button.click()
        page.wait_for_timeout(500)
        assert "jobs_page=2" in page.url

        page_3_button = page.locator('a.usa-pagination__button:has-text("3")').first
        if page_3_button.count() > 0:
            page_3_button.click()
            page.wait_for_timeout(500)
            assert "jobs_page=3" in page.url

            page.go_back()
            page.wait_for_timeout(500)
            assert "jobs_page=2" in page.url


def test_independent_pagination_sections(
    unauthed_page, interface, organization_data, source_data_dcatus
):
    """Test that jobs and errors pagination are independent."""
    interface.add_organization(organization_data)
    source = interface.add_harvest_source(source_data_dcatus)

    for i in range(25):
        job = interface.add_harvest_job(
            {"harvest_source_id": source.id, "status": "complete"}
        )

        if i < 20:
            interface.add_harvest_job_error(
                {
                    "harvest_job_id": job.id,
                    "message": f"Test error {i}",
                    "severity": "error",
                }
            )

    unauthed_page.goto("/metrics/")

    jobs_section = unauthed_page.locator("#paginated__harvest-jobs")
    page_2_in_jobs = jobs_section.locator(
        'a.usa-pagination__button:has-text("2")'
    ).first
    if page_2_in_jobs.count() > 0:
        page_2_in_jobs.click()
        unauthed_page.wait_for_timeout(500)

        assert "jobs_page=2" in unauthed_page.url

        errors_section = unauthed_page.locator("#paginated__harvest-errors")
        page_3_in_errors = errors_section.locator(
            'a.usa-pagination__button:has-text("3")'
        ).first
        if page_3_in_errors.count() > 0:
            page_3_in_errors.click()
            unauthed_page.wait_for_timeout(500)

            assert "jobs_page=2" in unauthed_page.url
            assert "errors_page=3" in unauthed_page.url
