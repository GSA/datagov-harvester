import os
import uuid

import pytest

api_token = os.getenv("HARVEST_API_TOKEN")


@pytest.fixture()
def apage(authed_page):
    authed_page.goto("")
    yield authed_page


class TestHarvestAPICreateAndDestroy:
    def test_api_create_and_destroy_org(self, apage):
        org_id = str(uuid.uuid4())
        fixture_org = {
            "name": "Test Org New",
            "slug": f"test-org-{org_id[:8]}",
            "id": org_id,
        }
        res = apage.request.post(
            "/api/v1/organization/add",
            headers={
                "X-API-Key": api_token,
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
            f"/api/v1/organization/{org_id}",
            headers={
                "X-API-Key": api_token,
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
        suffix = org_id[:8]
        fixture_org = {
            "name": f"Test Org New {suffix}",
            "slug": f"test-org-{suffix}",
            "id": org_id,
        }
        res = apage.request.post(
            "/api/v1/organization/add",
            headers={
                "X-API-Key": api_token,
                "Content-Type": "application/json",
            },
            data=fixture_org,
        )
        fixture_source = {
            "id": source_id,
            "name": f"Test Source New {suffix}",
            "notification_emails": ["email@example.com"],
            "organization_id": org_id,
            "frequency": "manual",
            "url": f"http://localhost:80/dcatus/dcatus_2_{suffix}.json",
            "schema_type": "dcatus1.1: federal",
            "source_type": "document",
            "notification_frequency": "always",
        }
        res = apage.request.post(
            "/api/v1/harvest_source/add",
            headers={
                "X-API-Key": api_token,
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
            f"/api/v1/harvest_source/{source_id}",
            headers={
                "X-API-Key": api_token,
                "Content-Type": "application/json",
            },
        )
        assert res.status == 202
        assert (
            res.json()["message"] == "This harvest source may take some time to delete."
        )
