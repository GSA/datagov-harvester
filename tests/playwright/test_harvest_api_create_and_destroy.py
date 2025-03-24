import os

import pytest

api_token = os.getenv("FLASK_APP_SECRET_KEY")


@pytest.fixture()
def apage(authed_page):
    authed_page.goto("")
    yield authed_page


class TestHarvestAPICreateAndDestroy:
    def test_api_create_and_destroy_org(self, apage):
        fixture_org = {
            "name": "Test Org New",
            "id": "123456789101112",
        }
        res = apage.request.post(
            "/organization/add",
            headers={
                "Authorization": api_token,
                "Content-Type": "application/json",
            },
            data=fixture_org,
        )
        assert res.status == 200
        assert (
            # ruff: noqa: E501
            res.json()["message"]
            == "Added new organization with ID: 123456789101112"
        )
        res = apage.request.delete(
            "/organization/123456789101112",
            headers={
                "Authorization": api_token,
                "Content-Type": "application/json",
            },
        )
        assert res.status == 200
        assert (
            # ruff: noqa: E501
            res.json()["message"]
            == "Deleted organization with ID:123456789101112 successfully"
        )

    def test_api_create_and_destroy_harvest_source(self, apage):
        fixture_source = {
            "id": "123456789101112",
            "name": "Test Source New",
            "notification_emails": ["email@example.com"],
            "organization_id": "d925f84d-955b-4cb7-812f-dcfd6681a18f",
            "frequency": "manual",
            "url": "http://localhost:80/dcatus/dcatus_2.json",
            "schema_type": "dcatus1.1: federal",
            "source_type": "document",
            "notification_frequency": "always",
        }
        res = apage.request.post(
            "/harvest_source/add",
            headers={
                "Authorization": api_token,
                "Content-Type": "application/json",
            },
            data=fixture_source,
        )
        assert res.status == 200
        assert (
            # ruff: noqa: E501
            res.json()["message"]
            == "Added new harvest source with ID: 123456789101112. No job scheduled for manual source."
        )
        res = apage.request.delete(
            "/harvest_source/123456789101112",
            headers={
                "Authorization": api_token,
                "Content-Type": "application/json",
            },
        )
        assert res.status == 200
        assert (
            res.json()["message"]
            == "Deleted harvest source with ID:123456789101112 successfully"
        )
