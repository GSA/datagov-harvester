import pytest


@pytest.fixture()
def organization(interface, organization_data):
    yield interface.add_organization(organization_data)


@pytest.fixture()
def source(interface, organization, source_data_dcatus):
    yield interface.add_harvest_source(source_data_dcatus)


@pytest.fixture
def job(interface, source):
    """A harvest job for the fixtured source."""
    yield interface.add_harvest_job(
        {
            "harvest_source_id": source.id,
            "status": "complete",
        }
    )
