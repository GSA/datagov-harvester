import os
import uuid

import pytest

api_token = os.getenv("FLASK_APP_SECRET_KEY")


@pytest.fixture()
def apage(authed_page):
    authed_page.goto("")
    yield authed_page


class TestHarvestAPICreateAndDestroy:
    def test_api_create_and_destroy_org(self, apage):
        org_id = str(uuid.uuid4())
        fixture_org = {
            "name": "Test Org New",
            "id": org_id
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
            == f"Added new organization with ID: {org_id}"
        )
        res = apage.request.delete(
            f"/organization/{org_id}",
            headers={
                "Authorization": api_token,
                "Content-Type": "application/json",
            },
        )
        assert res.status == 200
        assert (
            # ruff: noqa: E501
            res.json()["message"]
            == f"Deleted organization with ID:{org_id} successfully"
        )

    def test_api_create_and_destroy_harvest_source(self, apage):
        source_id = str(uuid.uuid4())
        org_id = str(uuid.uuid4())
        fixture_org = {
            "name": "Test Org New",
            "id": org_id
        }
        res = apage.request.post(
            "/organization/add",
            headers={
                "Authorization": api_token,
                "Content-Type": "application/json",
            },
            data=fixture_org,
        )
        fixture_source = {
            "id": source_id,
            "name": "Test Source New",
            "notification_emails": ["email@example.com"],
            "organization_id": org_id,
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
            == f"Added new harvest source with ID: {source_id}. No job scheduled for manual source."
        )
        res = apage.request.delete(
            f"/harvest_source/{source_id}",
            headers={
                "Authorization": api_token,
                "Content-Type": "application/json",
            },
        )
        assert res.status == 200
        assert (
            res.json()["message"]
            == f"Deleted harvest source with ID:{source_id} successfully"
        )
