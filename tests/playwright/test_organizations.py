import re
from playwright.sync_api import expect

add_org_button_string = "Add Organization"


def test_not_logged_in(unauthed_page, base_url):
    unauthed_page.goto(f"{base_url}/organizations")
    expect(unauthed_page.locator(f"text={add_org_button_string}")).to_have_count(0)


def test_logged_in(authed_page, base_url):
    authed_page.goto(f"{base_url}/organizations")
    expect(authed_page.locator(f"text={add_org_button_string}")).to_have_count(1)
