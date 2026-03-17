import re

import pytest
from playwright.sync_api import expect


@pytest.fixture()
def upage(unauthed_page):
    unauthed_page.goto("/dataset/fixture-dataset-1")
    yield unauthed_page


@pytest.fixture()
def apage(authed_page):
    authed_page.goto("/dataset/fixture-dataset-1")
    yield authed_page


class TestDatasetUnauthed:
    """
    Test an unauthed view of the dataset detail page.
    """

    def test_config_table_properties(self, upage):
        """
        Test that the dataset table is populated witht he expected info.
        """
        expect(
            upage.locator(".config-table.dataset-config-properties table tr td")
        ).to_have_text(
            [
                "ID:",
                "a1b2c3d4-e5f6-7890-abcd-ef1234567891",
                "Slug:",
                "fixture-dataset-1",
                "Harvest Source:",
                "2f2652de-91df-4c63-8b53-bfced20b276b",
                "Organization:",
                "Test Org",
                "Popularity:",
                "N/A",
                "Last Harvested:",
                re.compile(
                    r"\d{4}-\d{2}-\d{2} \d{1,2}:\d{2} [AP]M \(GMT(?:[+-]\d{1,2})?\)"
                ),
            ],
            use_inner_text=True,
        )

    def test_dataset_actions_hidden(self, upage):
        action_section = upage.locator(".config-actions.dataset-config-actions.mt-3")
        expect(action_section).not_to_be_visible()


class TestDatasetAuthed:
    """
    Test authed view of dataset detail page.
    The details of the dataset should be visible along with the action to edit
    the slug of a dataset.
    """

    def test_config_table_properties(self, apage):
        """
        Test that the dataset table is populated witht he expected info.
        """
        expect(
            apage.locator(".config-table.dataset-config-properties table tr td")
        ).to_have_text(
            [
                "ID:",
                "a1b2c3d4-e5f6-7890-abcd-ef1234567891",
                "Slug:",
                "fixture-dataset-1",
                "Harvest Source:",
                "2f2652de-91df-4c63-8b53-bfced20b276b",
                "Organization:",
                "Test Org",
                "Popularity:",
                "N/A",
                "Last Harvested:",
                re.compile(
                    r"\d{4}-\d{2}-\d{2} \d{1,2}:\d{2} [AP]M \(GMT(?:[+-]\d{1,2})?\)"
                ),
            ],
            use_inner_text=True,
        )

    def test_dataset_actions_visible(self, apage):
        """
        Test actions are visible, currently the only action is to update slug.
        """
        action_section = apage.locator(".config-actions.dataset-config-actions.mt-3")
        expect(action_section).to_be_visible()
        actions = apage.locator(".config-actions.dataset-config-actions.mt-3 button")
        expect(actions).to_have_text(["Update Slug"])
