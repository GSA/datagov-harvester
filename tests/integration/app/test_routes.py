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
