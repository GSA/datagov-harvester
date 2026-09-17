from unittest.mock import MagicMock, Mock

import pytest

from harvester.harvest import Record


@pytest.fixture
def mock_record():
    record = Record.__new__(Record)
    record._harvest_source = MagicMock()
    record._harvest_source.db_interface = MagicMock()
    record._harvest_source.update_job_record_count_by_action = MagicMock()
    record._identifier = "test-id"
    record._action = None
    record._status = None
    record._dataset_slug = "test-slug"
    record._id = "record-123"
    record._date_finished = None
    record._ckan_id = None
    record.update_self_in_db = MagicMock()
    record._metadata_for_dataset = MagicMock(return_value={"title": "Test"})
    record._dataset_payload = MagicMock(return_value={"slug": "test-slug"})
    record._index_dataset_in_opensearch = MagicMock()
    record._insert_dataset_with_unique_slug = MagicMock(return_value=Mock(id="ds-1"))
    record._delete_dataset_from_opensearch = MagicMock()
    return record


class TestRecordSyncStatus:
    def test_sync_sets_dataset_pending_initially(self, mock_record):
        """Test that status is set to dataset_pending when dataset operation fails"""
        mock_record._action = "create"
        mock_record._insert_dataset_with_unique_slug.return_value = None
        mock_record.sync()

        assert mock_record._status == "dataset_pending"

    def test_sync_sets_success_after_create(self, mock_record):
        """Test that status is set to success after successful dataset create"""
        mock_record._action = "create"
        mock_record._insert_dataset_with_unique_slug.return_value = Mock(id="ds-1")

        result = mock_record.sync()

        assert result is True
        assert mock_record._status == "success"

    def test_sync_sets_success_after_update(self, mock_record):
        """Test that status is set to success after successful dataset update"""
        mock_record._action = "update"
        mock_record._harvest_source.db_interface.upsert_dataset.return_value = Mock(
            id="ds-1"
        )

        result = mock_record.sync()

        assert result is True
        assert mock_record._status == "success"

    def test_sync_sets_success_after_delete(self, mock_record):
        """Test that status is set to success after successful dataset delete"""
        mock_record._action = "delete"
        mock_record._harvest_source.db_interface.get_dataset_by_slug.return_value = (
            Mock(id="ds-1")
        )
        mock_record._harvest_source.db_interface.delete_dataset_by_slug.return_value = (
            True
        )

        result = mock_record.sync()

        assert result is True
        assert mock_record._status == "success"
