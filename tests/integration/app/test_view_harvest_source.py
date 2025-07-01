class TestViewSource:

    def test_org_name(self, client, source, organization):
        """Organization name is linked."""
        resp = client.get(f"/harvest_source/{source.id}")
        assert organization.name in resp.text
        assert f'href="/organization/{organization.id}"' in resp.text
