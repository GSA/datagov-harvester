import pytest


class TestViewSource:

    def test_org_name(self, client, source, organization):
        """Organization name is linked."""
        url = f'/harvest_source/{source.id}'
        print(url)
        resp = client.get(url)
        print(resp.text)
        assert organization.name in resp.text
        assert f'href="/organization/{organization.id}"' in resp.text
