import os

class TestRoutes:
    def test_get_home(
        self,
        client,
    ):
        res = client.get("/")
        assert res.status_code == 302  # redirects to /organizations

    def test_get_organizations(
        self,
        client,
    ):
        res = client.get("/organizations/")
        assert res.status_code == 200

    def test_get_organization(
        self,
        client,
        interface_with_multiple_jobs,
        organization_data,
    ):
        res = client.get(f"/organization/{organization_data['id']}")
        assert res.status_code == 200

    def test_get_harvest_sources(
        self,
        client,
    ):
        res = client.get("/harvest_sources/")
        assert res.status_code == 200

    def test_get_harvest_source(
        self, client, interface_with_multiple_jobs, source_data_dcatus
    ):
        res = client.get(f"/harvest_source/{source_data_dcatus['id']}")
        assert res.status_code == 200

    def test_get_metrics(
        self,
        client,
    ):
        res = client.get("/metrics/")
        assert res.status_code == 200

    def test_add_organization_requires_login(self, client):
        res = client.get("/organization/add")
        assert res.status_code == 302

    def test_login_required_no_token(self, client):
        headers = {"Content-Type": "application/json"}
        data = {"name": "Test Org", "logo": "test_logo.png"}
        response = client.get("/organization/add", json=data, headers=headers)
        assert response.status_code == 401
        assert response.data.decode() == "error: Authorization header missing"

    def test_login_required_valid_token(self, client):
        api_token = os.getenv("FLASK_APP_SECRET_KEY")
        headers = {
            "Authorization": api_token,
            "Content-Type": "application/json",
        }
        data = {"name": "Test Org", "logo": "test_logo.png"}
        response = client.get("/organization/add", json=data, headers=headers)
        assert response.status_code == 200

    def test_login_required_invalid_token(self, client):
        headers = {
            "Authorization": "invalid_token",
            "Content-Type": "application/json",
        }
        data = {"name": "Test Org", "logo": "test_logo.png"}
        response = client.get("/organization/add", json=data, headers=headers)
        assert response.status_code == 401
        assert response.data.decode() == "error: Unauthorized"
