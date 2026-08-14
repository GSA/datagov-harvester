import pytest

from harvester.harvest import HarvestSource


@pytest.fixture
def make_harvest_source(interface, organization_data):
    """Seed an organization, harvest source, and harvest job, and return a
    HarvestSource for the job.

    Usage: `harvest_source = make_harvest_source(source_data_x, job_data_x)`
    """

    def _make(source_data, job_data):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data)
        harvest_job = interface.add_harvest_job(job_data)
        return HarvestSource(harvest_job.id)

    return _make
