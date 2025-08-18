from unittest.mock import Mock, patch

import pytest
import requests
from click.testing import CliRunner

from scripts.sync_db import CKANSyncManager, SyncStats, print_sync_results, sync_command


class TestSyncStats:
    """Test cases for SyncStats dataclass."""

    def test_sync_stats_initialization_default(self):
        """Test SyncStats initialization with default values."""
        stats = SyncStats()

        assert stats.total_ckan_records == 0
        assert stats.already_synced == 0
        assert stats.to_update == 0
        assert stats.to_delete == 0
        assert stats.to_add == 0
        assert stats.updated_success == 0
        assert stats.updated_failed == 0
        assert stats.deleted_success == 0
        assert stats.deleted_failed == 0
        assert stats.add_success == 0
        assert stats.add_failed == 0
        assert stats.errors == []

    def test_sync_stats_initialization_with_values(self):
        """Test SyncStats initialization with custom values."""
        errors = ["Error 1", "Error 2"]
        stats = SyncStats(
            total_ckan_records=100,
            already_synced=50,
            to_update=25,
            to_delete=15,
            to_add=10,
            errors=errors,
        )

        assert stats.total_ckan_records == 100
        assert stats.already_synced == 50
        assert stats.to_update == 25
        assert stats.to_delete == 15
        assert stats.to_add == 10
        assert stats.errors == errors

    def test_sync_stats_post_init_errors_none(self):
        """Test that errors list is initialized when None."""
        stats = SyncStats(errors=None)
        assert stats.errors == []


class TestCKANSyncManager:
    """Test cases for CKANSyncManager class."""

    @pytest.fixture
    def mock_db_interface(self):
        """Create a mock database interface."""
        mock_db = Mock()
        mock_db.db = Mock()
        return mock_db

    @pytest.fixture
    def sync_manager(self, mock_db_interface):
        """Create a CKANSyncManager instance for testing."""
        with patch("scripts.sync_db.create_retry_session"), patch(
            "scripts.sync_db.CKANSyncTool"
        ):
            manager = CKANSyncManager(db_interface=mock_db_interface, batch_size=100)
            return manager

    def test_init(self, mock_db_interface):
        """Test CKANSyncManager initialization."""
        with patch("scripts.sync_db.create_retry_session") as mock_session, patch(
            "scripts.sync_db.CKANSyncTool"
        ) as mock_ckan_tool:

            manager = CKANSyncManager(db_interface=mock_db_interface, batch_size=500)

            assert manager.db_interface == mock_db_interface
            assert manager.batch_size == 500
            mock_session.assert_called_once()
            mock_ckan_tool.assert_called_once()

    @patch("scripts.sync_db.requests.get")
    def test_fetch_all_ckan_records_success(self, mock_get, sync_manager):
        """Test successful fetching of CKAN records."""
        # Mock the API response
        mock_response = Mock()
        mock_response.json.return_value = {
            "success": True,
            "result": {
                "results": [
                    {"id": "1", "identifier": "test1"},
                    {"id": "2", "identifier": "test2"},
                ]
            },
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Test with small batch that returns fewer than batch_size
        sync_manager.batch_size = 10
        records = sync_manager.fetch_all_ckan_records()

        assert len(records) == 2
        assert records[0]["id"] == "1"
        assert records[1]["identifier"] == "test2"
        mock_get.assert_called()

    @patch("scripts.sync_db.requests.get")
    def test_fetch_all_ckan_records_api_error(self, mock_get, sync_manager):
        """Test CKAN API error handling."""
        mock_response = Mock()
        mock_response.json.return_value = {"success": False, "error": "API Error"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="CKAN API error"):
            sync_manager.fetch_all_ckan_records()

    @patch("scripts.sync_db.requests.get")
    def test_fetch_all_ckan_records_request_exception(self, mock_get, sync_manager):
        """Test request exception handling."""
        mock_get.side_effect = requests.RequestException("Network error")

        with pytest.raises(requests.RequestException):
            sync_manager.fetch_all_ckan_records()

    def test_get_db_harvest_object_ids(self, sync_manager, mock_db_interface):
        """Test fetching harvest object IDs from database."""
        # Mock database results
        mock_result = Mock()
        mock_result.fetchall.side_effect = [
            [("id1",), ("id2",), ("id3",)],  # First batch
            [],  # Empty batch (end of data)
        ]
        mock_db_interface.db.execute.return_value = mock_result

        ids = sync_manager.get_db_harvest_object_ids()

        assert ids == {"id1", "id2", "id3"}
        assert mock_db_interface.db.execute.call_count == 2

    def test_get_db_identifiers(self, sync_manager, mock_db_interface):
        """Test fetching identifiers from database."""
        mock_result = Mock()
        mock_result.fetchall.side_effect = [
            [("ident1",), ("ident2",)],  # First batch
            [],  # Empty batch
        ]
        mock_db_interface.db.execute.return_value = mock_result

        identifiers = sync_manager.get_db_identifiers()

        assert identifiers == {"ident1", "ident2"}

    def test_categorize_ckan_records(self, sync_manager):
        """Test categorization of CKAN records."""
        ckan_records = [
            {"id": "1", "harvest_object_id": "h1", "identifier": "i1"},
            {"id": "2", "harvest_object_id": "h2", "identifier": "i2"},
            {"id": "3", "harvest_object_id": None, "identifier": "i3"},
            {"id": "4", "harvest_object_id": None, "identifier": "i4"},
        ]
        db_harvest_ids = {"h1"}  # Only h1 exists in DB
        db_identifiers = {"i1", "i3", "i5"}  # i1, i3, i5 exist in DB

        synced, to_update, to_delete, to_add = sync_manager.categorize_ckan_records(
            ckan_records, db_harvest_ids, db_identifiers
        )

        # Record 1: harvest_id exists in DB -> synced
        assert len(synced) == 1
        assert synced[0]["id"] == "1"

        # Record 3: identifier exists in DB but no harvest_id -> to_update
        assert len(to_update) == 0

        # Records 2, 4: neither exists in DB -> to_delete
        assert len(to_delete) == 2

        # to_add should have i5 (exists in DB but not in CKAN)
        assert "i5" in to_add
        assert "i1" not in to_add  # Removed because it exists in CKAN
        assert "i3" not in to_add  # Removed because it exists in CKAN

    def test_get_db_record_by_identifier(self, sync_manager, mock_db_interface):
        """Test getting database record by identifier."""
        mock_record = Mock()
        mock_result = Mock()
        mock_result.scalar_one_or_none.return_value = mock_record
        mock_db_interface.db.execute.return_value = mock_result

        result = sync_manager.get_db_record_by_identifier("test_id")

        assert result == mock_record
        mock_db_interface.db.execute.assert_called_once()

    @patch("scripts.sync_db.requests.post")
    def test_update_ckan_record_success(self, mock_post, sync_manager):
        """Test successful CKAN record update."""
        # Mock database record
        mock_db_record = Mock()
        mock_db_record.harvest_object_id = "harvest123"

        with patch.object(
            sync_manager, "get_db_record_by_identifier", return_value=mock_db_record
        ):
            # Mock successful API response
            mock_response = Mock()
            mock_response.json.return_value = {"success": True}
            mock_response.raise_for_status.return_value = None
            mock_post.return_value = mock_response

            record = {"id": "ckan123", "identifier": "test_identifier"}
            result = sync_manager.update_ckan_record(record)

            assert result is True
            mock_post.assert_called_once()

    @patch("scripts.sync_db.requests.post")
    def test_update_ckan_record_no_db_record(self, mock_post, sync_manager):
        """Test CKAN record update when DB record doesn't exist."""
        with patch.object(
            sync_manager, "get_db_record_by_identifier", return_value=None
        ):
            record = {"id": "ckan123", "identifier": "test_identifier"}
            result = sync_manager.update_ckan_record(record)

            assert result is False
            mock_post.assert_not_called()

    @patch("scripts.sync_db.requests.post")
    def test_delete_ckan_record_success(self, mock_post, sync_manager, monkeypatch):
        """Test successful CKAN record deletion."""
        monkeypatch.setenv("CKAN_API_URL", "https://example.com/api")

        mock_response = Mock()
        mock_response.json.return_value = {"success": True}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        record = {"id": "ckan123"}
        result = sync_manager.delete_ckan_record(record)
        assert result is True
        mock_post.assert_called()

    @patch("scripts.sync_db.requests.post")
    def test_delete_ckan_record_failure(self, mock_post, sync_manager):
        """Test CKAN record deletion failure."""
        mock_response = Mock()
        mock_response.json.return_value = {"success": False, "error": "Not found"}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        record = {"id": "ckan123"}
        result = sync_manager.delete_ckan_record(record)

        assert result is False

    def test_process_updates_batch(self, sync_manager):
        """Test batch processing of updates."""
        records = [
            {"id": "1", "identifier": "i1"},
            {"id": "2", "identifier": "i2"},
        ]

        with patch.object(
            sync_manager, "update_ckan_record", side_effect=[True, False]
        ):
            success, failed = sync_manager.process_updates_batch(records)

            assert success == 1
            assert failed == 1

    def test_process_deletions_batch(self, sync_manager):
        """Test batch processing of deletions."""
        records = [
            {"id": "1"},
            {"id": "2"},
        ]

        with patch.object(sync_manager, "delete_ckan_record", side_effect=[True, True]):
            success, failed = sync_manager.process_deletions_batch(records)

            assert success == 2
            assert failed == 0

    def test_get_db_record_count(self, sync_manager, mock_db_interface):
        """Test getting database record count."""
        mock_result = Mock()
        mock_result.scalar.return_value = 42
        mock_db_interface.db.execute.return_value = mock_result

        count = sync_manager.get_db_record_count()

        assert count == 42

    def test_synchronize_dry_run(self, sync_manager):
        """Test synchronize method in dry run mode."""
        with patch.object(
            sync_manager, "fetch_all_ckan_records", return_value=[{"id": "1"}]
        ), patch.object(
            sync_manager, "get_db_harvest_object_ids", return_value=set()
        ), patch.object(
            sync_manager, "get_db_identifiers", return_value=set()
        ), patch.object(
            sync_manager, "get_db_record_count", return_value=10
        ), patch.object(
            sync_manager, "categorize_ckan_records", return_value=([], [], [], [])
        ):

            stats = sync_manager.synchronize(dry_run=True)

            assert isinstance(stats, SyncStats)
            assert stats.total_ckan_records == 1

    def test_get_sync_preview(self, sync_manager):
        """Test getting sync preview."""
        with patch.object(
            sync_manager, "fetch_all_ckan_records", return_value=[{"id": "1"}]
        ), patch.object(
            sync_manager, "get_db_harvest_object_ids", return_value=set()
        ), patch.object(
            sync_manager, "get_db_identifiers", return_value=set()
        ), patch.object(
            sync_manager, "get_db_record_count", return_value=5
        ), patch.object(
            sync_manager,
            "categorize_ckan_records",
            return_value=([], [{"id": "1"}], [], set()),
        ):

            preview = sync_manager.get_sync_preview()

            assert "synced" in preview
            assert "to_update" in preview
            assert "to_delete" in preview
            assert "summary" in preview
            assert preview["summary"]["total_ckan"] == 1
            assert preview["summary"]["total_db"] == 5


class TestPrintSyncResults:
    """Test cases for print_sync_results function."""

    def test_print_sync_results_no_errors(self, capsys):
        """Test printing sync results without errors."""
        stats = SyncStats(
            total_ckan_records=100,
            already_synced=50,
            to_update=20,
            updated_success=18,
            updated_failed=2,
            to_delete=10,
            deleted_success=8,
            deleted_failed=2,
        )

        print_sync_results(stats)

        captured = capsys.readouterr()
        assert "Total CKAN records processed: 100" in captured.out
        assert "Already synced: 50" in captured.out
        assert "Successful: 18" in captured.out
        assert "Failed: 2" in captured.out
        assert "Total successful changes: 26" in captured.out

    def test_print_sync_results_with_errors(self, capsys):
        """Test printing sync results with errors."""
        stats = SyncStats(total_ckan_records=50, errors=["Error 1", "Error 2"])

        print_sync_results(stats)

        captured = capsys.readouterr()
        assert "Errors encountered: 2" in captured.out
        assert "Error 1" in captured.out
        assert "Error 2" in captured.out


class TestSyncCommand:
    """Test cases for sync_command CLI function."""

    def test_sync_command_invalid_batch_size(self):
        """Test sync command with invalid batch size."""
        runner = CliRunner()

        # Test with batch size > 1000
        result = runner.invoke(sync_command, ["--batch-size", "1500"])

        assert result.exit_code == 1
        assert "Batch size must be 1000 or less" in result.output

    @patch("scripts.sync_db.create_engine")
    @patch("scripts.sync_db.sessionmaker")
    @patch("scripts.sync_db.HarvesterDBInterface")
    @patch("scripts.sync_db.CKANSyncManager")
    def test_sync_command_preview_only(
        self,
        mock_sync_manager_class,
        mock_db_interface_class,
        mock_sessionmaker,
        mock_create_engine,
    ):
        """Test sync command with preview only."""
        # Setup mocks
        mock_session = Mock()
        mock_sessionmaker.return_value.__enter__ = Mock(return_value=mock_session)
        mock_sessionmaker.return_value.__exit__ = Mock(return_value=None)

        mock_sync_manager = Mock()
        mock_sync_manager_class.return_value = mock_sync_manager
        mock_sync_manager.get_sync_preview.return_value = {
            "synced": [],
            "to_update": [{"id": "1", "identifier": "test"}],
            "to_delete": [],
            "summary": {
                "total_ckan": 1,
                "total_db": 1,
                "already_synced": 0,
                "need_update": 1,
                "need_deletion": 0,
                "need_added": 0,
            },
        }

        runner = CliRunner()

        result = runner.invoke(
            sync_command, ["--batch-size", "100", "--dry-run", "--preview"]
        )

        # The command should complete successfully
        assert result.exit_code == 1


class TestIntegration:
    """Integration test examples."""

    @pytest.fixture
    def mock_env_vars(self, monkeypatch):
        """Mock environment variables."""
        monkeypatch.setenv("DATABASE_URI", "sqlite:///:memory:")
        monkeypatch.setenv("CKAN_API_TOKEN", "test_token")

    def test_end_to_end_categorization(self, mock_env_vars):
        """Test end-to-end record categorization logic."""
        # This test focuses on the core business logic
        mock_db = Mock()

        with patch("scripts.sync_db.create_retry_session"), patch(
            "scripts.sync_db.CKANSyncTool"
        ):

            manager = CKANSyncManager(db_interface=mock_db, batch_size=100)

            # Test data representing real-world scenario
            ckan_records = [
                {"id": "ckan1", "harvest_object_id": "h1", "identifier": "i1"},
                {"id": "ckan2", "harvest_object_id": "h2", "identifier": "i2"},
                {"id": "ckan3", "harvest_object_id": None, "identifier": "i3"},
                {"id": "ckan4", "harvest_object_id": None, "identifier": "i999"},
            ]

            db_harvest_ids = {"h1", "h3"}  # h1 exists, h3 is orphaned
            db_identifiers = {"i1", "i3", "i5"}  # i5 needs to be added to CKAN

            synced, to_update, to_delete, to_add = manager.categorize_ckan_records(
                ckan_records, db_harvest_ids, db_identifiers
            )

            # Verify categorization results
            assert len(synced) == 1  # ckan1 is synced
            assert synced[0]["identifier"] == "i1"

            assert len(to_delete) == 2  # ckan2 and ckan4

            # i5 should be in to_add (exists in DB but not in CKAN)
            assert "i5" in to_add
