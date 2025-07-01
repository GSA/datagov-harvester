class TestViewHarvestJob:

    def test_harvest_source_name(self, client, job):
        """The harvest source's name appear on the harvest job page."""
        resp = client.get(f"/harvest_job/{job.id}")
        assert job.source.name in resp.text
        assert f'a href="/harvest_source/{job.source.id}"' in resp.text
