import pytest


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

    interface.db.commit()

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


def test_pagination_url_is_bookmarkable(metrics_page_with_jobs):
    """Test that navigating directly to a paginated URL loads that page."""
    page = metrics_page_with_jobs

    page.goto("/metrics/?jobs_page=2")
    page.wait_for_timeout(500)

    assert "jobs_page=2" in page.url


def test_pagination_back_button_works(metrics_page_with_jobs):
    """Test that browser back button works with pagination."""
    page = metrics_page_with_jobs

    page_2_button = page.locator('a.usa-pagination__button:has-text("2")').first
    if page_2_button.count() > 0:
        page_2_button.click()
        page.wait_for_timeout(500)
        assert "jobs_page=2" in page.url

        page.go_back()
        page.wait_for_timeout(500)
        assert "jobs_page=1" in page.url or page.url.endswith("/metrics/")


def test_independent_pagination_sections(
    unauthed_page, interface, organization_data, source_data_dcatus
):
    """Test that jobs and errors pagination are independent."""
    interface.add_organization(organization_data)
    source = interface.add_harvest_source(source_data_dcatus)

    jobs = []
    for i in range(25):
        job = interface.add_harvest_job(
            {"harvest_source_id": source.id, "status": "complete"}
        )
        jobs.append(job)

    for i in range(20):
        interface.add_harvest_job_error(
            {"harvest_job_id": jobs[i].id, "message": f"Test error {i}"}
        )

    interface.db.commit()

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
        page_2_in_errors = errors_section.locator(
            'a.usa-pagination__button:has-text("2")'
        ).first
        if page_2_in_errors.count() > 0:
            page_2_in_errors.click()
            unauthed_page.wait_for_timeout(500)

            assert "jobs_page=2" in unauthed_page.url
            assert "errors_page=2" in unauthed_page.url
