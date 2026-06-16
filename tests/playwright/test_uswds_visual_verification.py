"""Visual verification of USWDS migration on pages that previously used Bootstrap."""

from pathlib import Path

import pytest
from playwright.sync_api import Page, expect

SCREENSHOT_DIR = Path(__file__).resolve().parent / "screenshots" / "uswds-migration"


def _shot(page: Page, name: str) -> Path:
    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    path = SCREENSHOT_DIR / f"{name}.png"
    page.screenshot(path=str(path), full_page=True)
    return path


def _assert_no_bootstrap_classes(page: Page) -> None:
    bootstrap_selectors = [
        ".btn.btn-primary",
        ".btn.btn-success",
        ".form-control",
        ".form-select",
        ".form-group",
        ".alert-warning",
        ".alert-success",
        ".alert-danger",
        ".btn-close",
        "[data-bs-dismiss]",
        ".list-unstyled",
        "table.table",
    ]
    for selector in bootstrap_selectors:
        expect(page.locator(selector)).to_have_count(0)


class TestUswdsVisualVerification:
    """Capture screenshots and sanity-check migrated markup on live pages."""

    def test_organization_list_and_add_button(self, authed_page: Page):
        page = authed_page
        page.goto("/organization_list/")
        expect(page.locator(".usa-button").first).to_be_visible()
        _assert_no_bootstrap_classes(page)
        _shot(page, "01-organization-list")

    def test_harvest_source_list_and_add_button(self, authed_page: Page):
        page = authed_page
        page.goto("/harvest_source_list/")
        expect(page.locator(".usa-button").first).to_be_visible()
        _assert_no_bootstrap_classes(page)
        _shot(page, "02-harvest-source-list")

    def test_add_organization_form(self, authed_page: Page):
        page = authed_page
        page.goto("/organization/add")
        expect(page.locator(".usa-form-group")).not_to_have_count(0)
        expect(page.locator(".usa-label")).not_to_have_count(0)
        expect(
            page.locator(".usa-input, .usa-select, .usa-textarea").first
        ).to_be_visible()
        expect(page.locator(".usa-button").first).to_be_visible()
        _assert_no_bootstrap_classes(page)
        _shot(page, "03-add-organization-form")

    def test_add_harvest_source_form(self, authed_page: Page):
        page = authed_page
        page.goto("/harvest_source/add")
        expect(page.locator(".usa-form-group")).not_to_have_count(0)
        expect(page.locator(".usa-select")).not_to_have_count(0)
        expect(page.locator(".usa-button").first).to_be_visible()
        _assert_no_bootstrap_classes(page)
        _shot(page, "04-add-harvest-source-form")

    def test_flash_alert_warning(self, authed_page: Page):
        page = authed_page
        page.goto("/organization/add")
        page.get_by_role("textbox", name="Name").fill("USWDS Visual Test Org")
        page.get_by_role("textbox", name="Slug").fill("uswds-visual-test-org")
        page.get_by_role("textbox", name="Logo").fill("https://example.com/logo.png")
        page.get_by_role("button", name="Submit").click()

        alert = page.locator(".usa-alert--warning")
        expect(alert).to_be_visible()
        expect(alert.locator(".js-alert-close")).to_be_visible()
        _assert_no_bootstrap_classes(page)
        _shot(page, "05-flash-alert-warning")

        alert.locator(".js-alert-close").click()
        expect(alert).to_have_count(0)
        _shot(page, "06-flash-alert-dismissed")

    def test_organization_detail_config_table(self, authed_page: Page):
        page = authed_page
        page.goto("/organization/d925f84d-955b-4cb7-812f-dcfd6681a18f")
        expect(page.locator(".config-table .usa-table")).to_be_visible()
        expect(page.locator(".usa-button-group")).to_be_visible()
        _assert_no_bootstrap_classes(page)
        _shot(page, "07-organization-detail")

    def test_harvest_source_detail_config_tables(self, authed_page: Page):
        page = authed_page
        page.goto("/harvest_source/2f2652de-91df-4c63-8b53-bfced20b276b")
        expect(page.locator(".config-table .usa-table")).not_to_have_count(0)
        expect(page.locator(".usa-button-group")).to_be_visible()
        _assert_no_bootstrap_classes(page)
        _shot(page, "08-harvest-source-detail")

    def test_harvest_job_detail(self, authed_page: Page):
        page = authed_page
        page.goto("/harvest_job/6bce761c-7a39-41c1-ac73-94234c139c76")
        expect(page.locator(".config-table .usa-table")).to_be_visible()
        _assert_no_bootstrap_classes(page)
        _shot(page, "09-harvest-job-detail")

    def test_dataset_detail_and_slug_form(self, authed_page: Page):
        page = authed_page
        page.goto("/dataset/fixture-dataset-1")
        expect(page.locator(".config-table .usa-table")).to_be_visible()
        expect(page.locator(".config-actions .usa-form-group")).to_be_visible()
        expect(page.locator(".config-actions .usa-button")).to_be_visible()
        _assert_no_bootstrap_classes(page)
        _shot(page, "10-dataset-detail-slug-form")

    def test_metrics_dashboard(self, authed_page: Page):
        page = authed_page
        page.goto("/metrics/")
        expect(page.locator(".usa-table").first).to_be_visible()
        expect(page.locator(".text-base-light").first).to_be_visible()
        _assert_no_bootstrap_classes(page)
        _shot(page, "11-metrics-dashboard")

    def test_validator_form(self, unauthed_page: Page):
        page = unauthed_page
        page.goto("/validate/")
        expect(page.locator(".usa-alert--info")).to_be_visible()
        expect(page.locator(".usa-form-group")).not_to_have_count(0)
        expect(page.locator(".usa-select")).not_to_have_count(0)
        expect(page.locator(".usa-button").first).to_be_visible()
        _assert_no_bootstrap_classes(page)
        _shot(page, "12-validator-form")

    def test_validator_results_and_download(self, unauthed_page: Page):
        page = unauthed_page
        page.goto("/validate/")
        page.locator("input[name=url]").fill(
            "http://nginx-harvest-source/dcatus/dcatus_multiple_invalid.json"
        )
        page.locator("input[type=submit]").click()

        expect(page.locator(".error-list")).to_be_visible()
        expect(page.locator("#btn-download.usa-button")).to_be_visible()
        _assert_no_bootstrap_classes(page)
        _shot(page, "13-validator-results")

    def test_validator_upload_error(self, unauthed_page: Page, tmp_path):
        page = unauthed_page
        page.goto("/validate/")
        bad_file = tmp_path / "catalog.txt"
        bad_file.write_text('{"dataset": []}', encoding="utf-8")

        page.locator("select[name=fetch_method]").select_option("upload")
        page.locator("input[type=file][name=json_file]").set_input_files(str(bad_file))
        page.locator("input[type=submit]").click()

        expect(page.locator("#upload_field .usa-error-message")).to_be_visible()
        _assert_no_bootstrap_classes(page)
        _shot(page, "14-validator-upload-error")

    def test_base_layout_grid_container(self, unauthed_page: Page):
        page = unauthed_page
        page.goto("/organization_list/")
        expect(page.locator("header .grid-container")).to_be_visible()
        expect(page.locator(".grid-container.margin-y-3")).to_be_visible()
        _assert_no_bootstrap_classes(page)
        _shot(page, "15-base-layout")
