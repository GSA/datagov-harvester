"""Integration tests for code.json harvest workflow."""

import json
from unittest.mock import Mock, patch

import pytest

from harvester.harvest import HarvestSource, harvest_job_starter


@pytest.fixture
def mock_codejson_response():
    """Mock code.json response with 2 sample releases."""
    return {
        "version": "2.0.0",
        "agency": "TEST",
        "measurementType": {"method": "projects"},
        "releases": [
            {
                "name": "Test Project 1",
                "repositoryURL": "https://github.com/test/project-1",
                "description": "A test project for code.json harvesting",
                "permissions": {
                    "licenses": [{"URL": "https://opensource.org/licenses/MIT"}],
                    "usageType": "openSource",
                },
                "laborHours": 100,
                "tags": ["python", "testing"],
                "contact": {"email": "test@example.com", "name": "Test Contact"},
                "status": "Production",
                "vcs": "git",
                "languages": ["Python", "JavaScript"],
                "organization": "Test Organization",
                "date": {
                    "created": "2024-01-01",
                    "lastModified": "2024-06-01",
                },
            },
            {
                "name": "Test Project 2",
                "repositoryURL": "https://github.com/test/project-2",
                "description": "Another test project",
                "permissions": {
                    "licenses": [{"name": "Apache-2.0"}],
                    "usageType": "openSource",
                },
                "laborHours": 200,
                "tags": ["go", "api"],
                "contact": {"email": "mailto:test2@example.com"},
                "status": "Beta",
                "vcs": "git",
                "languages": ["Go"],
                "date": {
                    "created": "2024-02-01",
                    "lastModified": "2024-07-01",
                },
            },
        ],
    }


@pytest.fixture
def source_data_codejson(organization_data):
    """Harvest source fixture for code.json."""
    return {
        "id": "a1b2c3d4-e5f6-4a5b-9c8d-1e2f3a4b5c6d",
        "name": "Test Code.json Source",
        "notification_emails": ["test@example.com"],
        "organization_id": organization_data["id"],
        "frequency": "weekly",
        "url": "https://example.gov/code.json",
        "schema_type": "code.json",
        "source_type": "document",
        "notification_frequency": "always",
    }


@pytest.fixture
def job_data_codejson(source_data_codejson):
    """Harvest job fixture for code.json."""
    return {
        "id": "f1e2d3c4-b5a6-4c5d-8e9f-0a1b2c3d4e5f",
        "status": "new",
        "harvest_source_id": source_data_codejson["id"],
    }


class TestCodejsonHarvest:
    """Integration tests for full code.json harvest workflow."""

    def test_harvest_creates_datasets(
        self,
        make_harvest_source,
        source_data_codejson,
        job_data_codejson,
        mock_codejson_response,
        monkeypatch,
    ):
        """Test that harvesting code.json creates dataset records."""
        # Mock the download to return our test code.json
        mock_response = Mock(
            ok=True,
            status_code=200,
            text=json.dumps(mock_codejson_response),
            json=lambda: mock_codejson_response,
        )
        monkeypatch.setattr(
            "harvester.utils.general_utils.requests.get",
            lambda *args, **kwargs: mock_response,
        )

        # Create harvest source and run harvest
        harvest_source = make_harvest_source(source_data_codejson, job_data_codejson)
        harvest_job_starter(job_data_codejson["id"], "harvest")

        # Verify job completed successfully
        job = harvest_source.db_interface.get_harvest_job(job_data_codejson["id"])
        assert job.status == "complete"
        assert job.records_added == 2
        assert job.records_errored == 0

        # Verify datasets were created
        datasets = harvest_source.db_interface.get_datasets()
        assert len(datasets) == 2

        # Verify DCAT structure
        dataset_1 = [d for d in datasets if "project-1" in d.dcat["identifier"]][0]
        assert dataset_1.dcat["title"] == "Test Project 1"
        assert (
            dataset_1.dcat["description"] == "A test project for code.json harvesting"
        )
        assert "Python" in dataset_1.dcat["theme"]
        assert dataset_1.dcat["contactPoint"]["hasEmail"] == "mailto:test@example.com"

    def test_harvest_with_invalid_release_skips_and_continues(
        self,
        make_harvest_source,
        source_data_codejson,
        job_data_codejson,
        mock_codejson_response,
        monkeypatch,
    ):
        """Test that invalid releases are skipped but harvest continues."""
        # Add an invalid release (missing required field)
        invalid_release = {
            "name": "Invalid Project",
            "repositoryURL": "https://github.com/test/invalid",
            # Missing description - required field
            "permissions": {"usageType": "openSource"},
        }
        mock_codejson_response["releases"].append(invalid_release)

        mock_response = Mock(
            ok=True,
            status_code=200,
            text=json.dumps(mock_codejson_response),
            json=lambda: mock_codejson_response,
        )
        monkeypatch.setattr(
            "harvester.utils.general_utils.requests.get",
            lambda *args, **kwargs: mock_response,
        )

        harvest_source = make_harvest_source(source_data_codejson, job_data_codejson)
        harvest_job_starter(job_data_codejson["id"], "harvest")

        # Job should complete but have errors
        job = harvest_source.db_interface.get_harvest_job(job_data_codejson["id"])
        assert job.status == "complete"
        assert job.records_added == 2  # Valid records
        assert job.records_errored == 1  # Invalid record

    def test_harvest_with_duplicate_repository_urls(
        self,
        make_harvest_source,
        source_data_codejson,
        job_data_codejson,
        mock_codejson_response,
        monkeypatch,
    ):
        """Test that duplicate repositoryURLs are detected."""
        # Add duplicate with same repositoryURL
        duplicate_release = mock_codejson_response["releases"][0].copy()
        duplicate_release["name"] = "Duplicate Project"
        mock_codejson_response["releases"].append(duplicate_release)

        mock_response = Mock(
            ok=True,
            status_code=200,
            text=json.dumps(mock_codejson_response),
            json=lambda: mock_codejson_response,
        )
        monkeypatch.setattr(
            "harvester.utils.general_utils.requests.get",
            lambda *args, **kwargs: mock_response,
        )

        harvest_source = make_harvest_source(source_data_codejson, job_data_codejson)
        harvest_job_starter(job_data_codejson["id"], "harvest")

        # Job should complete with error for duplicate
        job = harvest_source.db_interface.get_harvest_job(job_data_codejson["id"])
        assert job.status == "complete"
        assert job.records_errored > 0

    def test_harvest_update_detection(
        self,
        interface,
        make_harvest_source,
        source_data_codejson,
        job_data_codejson,
        mock_codejson_response,
        monkeypatch,
    ):
        """Test that updates are detected via lastModified date."""
        mock_response = Mock(
            ok=True,
            status_code=200,
            text=json.dumps(mock_codejson_response),
            json=lambda: mock_codejson_response,
        )
        monkeypatch.setattr(
            "harvester.utils.general_utils.requests.get",
            lambda *args, **kwargs: mock_response,
        )

        # First harvest
        harvest_source = make_harvest_source(source_data_codejson, job_data_codejson)
        harvest_job_starter(job_data_codejson["id"], "harvest")

        job_1 = interface.get_harvest_job(job_data_codejson["id"])
        assert job_1.records_added == 2

        # Modify one release and run second harvest
        mock_codejson_response["releases"][0]["description"] = "Updated description"
        mock_codejson_response["releases"][0]["date"]["lastModified"] = "2024-08-01"

        # Create new job for second harvest
        job_data_2 = {
            "id": "a1a2a3a4-b5b6-4c5d-8e9f-0a1b2c3d4e5f",
            "status": "new",
            "harvest_source_id": source_data_codejson["id"],
        }
        interface.add_harvest_job(job_data_2)

        harvest_job_starter(job_data_2["id"], "harvest")

        # Verify update was detected
        job_2 = interface.get_harvest_job(job_data_2["id"])
        assert job_2.status == "complete"
        assert job_2.records_updated == 1
        assert job_2.records_added == 0

    def test_harvest_deletion_detection(
        self,
        interface,
        make_harvest_source,
        source_data_codejson,
        job_data_codejson,
        mock_codejson_response,
        monkeypatch,
    ):
        """Test that removed repositories are marked for deletion."""
        mock_response = Mock(
            ok=True,
            status_code=200,
            text=json.dumps(mock_codejson_response),
            json=lambda: mock_codejson_response,
        )
        monkeypatch.setattr(
            "harvester.utils.general_utils.requests.get",
            lambda *args, **kwargs: mock_response,
        )

        # First harvest with 2 releases
        harvest_source = make_harvest_source(source_data_codejson, job_data_codejson)
        harvest_job_starter(job_data_codejson["id"], "harvest")

        job_1 = interface.get_harvest_job(job_data_codejson["id"])
        assert job_1.records_added == 2

        # Second harvest with only 1 release (one removed)
        mock_codejson_response["releases"] = [mock_codejson_response["releases"][0]]

        job_data_2 = {
            "id": "b1b2b3b4-c5c6-4d5e-8f9a-0b1c2d3e4f5a",
            "status": "new",
            "harvest_source_id": source_data_codejson["id"],
        }
        interface.add_harvest_job(job_data_2)

        harvest_job_starter(job_data_2["id"], "harvest")

        # Verify deletion was detected
        job_2 = interface.get_harvest_job(job_data_2["id"])
        assert job_2.status == "complete"
        assert job_2.records_deleted == 1

    def test_harvest_with_empty_releases_array(
        self,
        make_harvest_source,
        source_data_codejson,
        job_data_codejson,
        monkeypatch,
    ):
        """Test that empty releases array is handled gracefully."""
        empty_codejson = {
            "version": "2.0.0",
            "agency": "TEST",
            "measurementType": {"method": "projects"},
            "releases": [],
        }

        mock_response = Mock(
            ok=True,
            status_code=200,
            text=json.dumps(empty_codejson),
            json=lambda: empty_codejson,
        )
        monkeypatch.setattr(
            "harvester.utils.general_utils.requests.get",
            lambda *args, **kwargs: mock_response,
        )

        harvest_source = make_harvest_source(source_data_codejson, job_data_codejson)
        harvest_job_starter(job_data_codejson["id"], "harvest")

        # Job should complete with no records
        job = harvest_source.db_interface.get_harvest_job(job_data_codejson["id"])
        assert job.status == "complete"
        assert job.records_added == 0
        assert job.records_errored == 0
