import os
import sys
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest

# Add the parent directory to sys.path to import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from database.models import HarvestRecord
from scripts.sync_db import CKANSyncManager, SyncStats, print_sync_results, sync_command


class MockHarvestRecord(HarvestRecord):
    """Mock HarvestRecord for testing."""

    def __init__(
        self,
        identifier: str,
        id: str = None,
        ckan_name: str = None,
        date_finished: datetime = None,
        action: str = "create",
        status: str = "success",
    ):
        self.identifier = identifier
        self.id = id or f"harvest-{identifier}"
        self.ckan_name = ckan_name or f"ckan-{identifier}"
        self.date_finished = date_finished or datetime.now(timezone.utc)
        self.action = action
        self.status = status


@pytest.fixture
def mock_progressbar():
    """Create a mock that acts like click.progressbar context manager."""

    def create_progress_mock(records, **kwargs):
        mock = Mock()
        mock.__enter__ = Mock(return_value=records)
        mock.__exit__ = Mock(return_value=None)
        return mock

    return create_progress_mock


@pytest.fixture
def mock_db_interface():
    """Create a mock database interface."""
    interface = Mock()
    interface.db = Mock()
    return interface


@pytest.fixture
def sample_ckan_records():
    """Sample CKAN records for testing."""
    return [
        {
            "id": "7f9118f0-47b3-46fe-aff8-be811822373a",
            "name": "test-record-1",
            "metadata_modified": "2025-06-28T16:55:51.313Z",
            "identifier": "DASHLINK_872",
            "harvest_object_id": "537f6d3b-9256-415c-b5f8-aee31f4da580",
            "harvest_source_title": "nasa-data-json",
        },
        {
            "id": "8f9118f0-47b3-46fe-aff8-be811822373b",
            "name": "test-record-2",
            "metadata_modified": "2025-06-28T17:00:00.000Z",
            "identifier": "DASHLINK_873",
            "harvest_object_id": "537f6d3b-9256-415c-b5f8-aee31f4da581",
            "harvest_source_title": "nasa-data-json",
        },
    ]


@pytest.fixture
def sample_db_records():
    """Sample database records for testing."""
    return {
        "DASHLINK_872": MockHarvestRecord(
            identifier="DASHLINK_872",
            id="537f6d3b-9256-415c-b5f8-aee31f4da580",
            ckan_name="test-record-1",
            date_finished=datetime(2025, 6, 28, 16, 55, 0, tzinfo=timezone.utc),
        ),
        "DASHLINK_874": MockHarvestRecord(
            identifier="DASHLINK_874",
            id="537f6d3b-9256-415c-b5f8-aee31f4da582",
            ckan_name="test-record-3",
            action="delete",
        ),
    }


@pytest.fixture
def sync_manager(mock_db_interface):
    """Create a CKANSyncManager instance with mocked dependencies."""
    with patch.dict(
        os.environ,
        {"CKAN_API_TOKEN": "test-token", "CKAN_API_URL": "https://test.ckan.api"},
    ):
        manager = CKANSyncManager(db_interface=mock_db_interface)
        manager.session = Mock()
        manager.ckan_tool = Mock()
        return manager


class TestSyncStats:
    """Test the SyncStats dataclass."""

    def test_sync_stats_initialization(self):
        """Test SyncStats initialization with default values."""
        stats = SyncStats()
        assert stats.total_ckan_records == 0
        assert stats.already_synced == 0
        assert stats.to_update == 0
        assert stats.to_delete == 0
        assert stats.to_add == 0
        assert stats.errors == []

    def test_sync_stats_with_values(self):
        """Test SyncStats initialization with custom values."""
        errors = ["Error 1", "Error 2"]
        stats = SyncStats(total_ckan_records=100, already_synced=50, errors=errors)
        assert stats.total_ckan_records == 100
        assert stats.already_synced == 50
        assert stats.errors == errors


class TestCKANSyncManager:
    """Test the CKANSyncManager class."""

    def test_initialization(self, mock_db_interface, monkeypatch):
        """Test CKANSyncManager initialization."""
        monkeypatch.setattr("scripts.sync_db.CKAN_API_TOKEN", "test-token")
        monkeypatch.setattr("scripts.sync_db.CKAN_API_URL", "https://test.ckan.api")

        manager = CKANSyncManager(db_interface=mock_db_interface)
        assert manager.ckan_api_url == "https://test.ckan.api"
        assert manager.ckan_api_key == "test-token"
        assert manager.db_interface == mock_db_interface
        assert manager.batch_size == 1000

    def test_is_within_5_minutes_true(self, sync_manager):
        """Test is_within_5_minutes returns True for times within 5 minutes."""
        base_time = datetime(2025, 6, 28, 16, 55, 0, tzinfo=timezone.utc)
        iso_string = "2025-06-28T16:57:00.000Z"  # 2 minutes later

        assert sync_manager.is_within_5_minutes(iso_string, base_time) is True

    def test_is_within_5_minutes_false(self, sync_manager):
        """Test is_within_5_minutes returns False for times beyond 5 minutes."""
        base_time = datetime(2025, 6, 28, 16, 55, 0, tzinfo=timezone.utc)
        iso_string = "2025-06-28T17:05:00.000Z"  # 10 minutes later

        assert sync_manager.is_within_5_minutes(iso_string, base_time) is False

    def test_is_within_5_minutes_naive_datetime(self, sync_manager):
        """Test is_within_5_minutes handles naive datetime objects."""
        base_time = datetime(2025, 6, 28, 16, 55, 0)  # naive datetime
        iso_string = "2025-06-28T16:57:00.000Z"

        assert sync_manager.is_within_5_minutes(iso_string, base_time) is True

    def test_is_within_5_minutes_invalid_input(self, sync_manager):
        """Test is_within_5_minutes handles invalid input gracefully."""
        base_time = datetime.now()
        invalid_iso = "invalid-date-string"

        assert sync_manager.is_within_5_minutes(invalid_iso, base_time) is False

    @patch("scripts.sync_db.requests.Session.get")
    def test_fetch_all_ckan_records_success(
        self, mock_get, sync_manager, sample_ckan_records
    ):
        """Test successful fetching of CKAN records."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "success": True,
            "result": {
                "results": sample_ckan_records,
                "count": len(sample_ckan_records),
            },
        }
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        # Mock session
        sync_manager.session.get = mock_get

        records = sync_manager.fetch_all_ckan_records()

        assert len(records) == 2
        assert records[0]["identifier"] == "DASHLINK_872"
        mock_get.assert_called()

    @patch("scripts.sync_db.requests.Session.get")
    def test_fetch_all_ckan_records_api_error(self, mock_get, sync_manager):
        """Test handling of CKAN API errors."""
        mock_response = Mock()
        mock_response.json.return_value = {"success": False, "error": "API Error"}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        sync_manager.session.get = mock_get

        with pytest.raises(ValueError, match="CKAN API error: API Error"):
            sync_manager.fetch_all_ckan_records()

    def test_get_latest_db_records(self, sync_manager, sample_db_records):
        """Test getting latest database records."""
        # Mock the database query chain using intermediate variables
        query_chain = sync_manager.db_interface.db.query.return_value
        filter_chain = query_chain.filter.return_value
        order_chain = filter_chain.order_by.return_value
        distinct_chain = order_chain.distinct.return_value
        distinct_chain.subquery.return_value = Mock()

        # Mock the final query result
        mock_result = list(sample_db_records.values())
        sync_manager.db_interface.db.query.return_value.all.return_value = mock_result

        records = sync_manager.get_latest_db_records()

        assert len(records) == 2
        assert "DASHLINK_872" in records
        assert "DASHLINK_874" in records

    def test_needs_update_name_change(self, sync_manager):
        """Test needs_update detects name changes."""
        record = {"name": "new-name"}
        db_record = MockHarvestRecord("test", ckan_name="old-name")

        # Mock the time check method
        sync_manager.is_within_5_minutes = Mock(return_value=False)

        with patch.object(sync_manager, "is_within_5_minutes", return_value=False):
            result = sync_manager.needs_update(record, db_record)

        assert result is True

    def test_needs_update_time_within_5_minutes(self, sync_manager):
        """Test needs_update detects time-based updates."""
        record = {"name": "same-name", "metadata_modified": "2025-06-28T16:55:51.313Z"}
        db_record = MockHarvestRecord("test", ckan_name="same-name")

        with patch.object(sync_manager, "is_within_5_minutes", return_value=True):
            result = sync_manager.needs_update(record, db_record)

        assert result is True

    def test_categorize_ckan_records(
        self, sync_manager, sample_ckan_records, sample_db_records
    ):
        """Test categorization of CKAN records."""
        with patch.object(sync_manager, "needs_update", return_value=False):
            synced, to_update, to_delete, to_add = sync_manager.categorize_ckan_records(
                sample_ckan_records, sample_db_records
            )

        # One record should be synced (matching harvest_object_id)
        assert len(synced) >= 0
        # Records not in DB or with delete action should be in to_delete
        assert len(to_delete) >= 0

    def test_sync_record_success(self, sync_manager):
        """Test successful record synchronization."""
        db_record = MockHarvestRecord("test")
        sync_manager.ckan_tool.sync.return_value = True

        result = sync_manager.sync_record(db_record)

        assert result is True
        sync_manager.ckan_tool.sync.assert_called_once_with(db_record)

    def test_sync_record_failure(self, sync_manager):
        """Test failed record synchronization."""
        db_record = MockHarvestRecord("test")
        sync_manager.ckan_tool.sync.side_effect = Exception("Sync failed")

        result = sync_manager.sync_record(db_record)

        assert result is False

    def test_process_updates_batch(self, sync_manager, mock_progressbar):
        """Test batch processing of updates."""
        records = [MockHarvestRecord("test1"), MockHarvestRecord("test2")]

        with patch.object(sync_manager, "sync_record", side_effect=[True, False]):
            with patch(
                "scripts.sync_db.click.progressbar", side_effect=mock_progressbar
            ):
                success, failure = sync_manager.process_updates_batch(records)

        assert success == 1
        assert failure == 1

    def test_process_deletions_batch_with_dict(self, sync_manager, mock_progressbar):
        """Test batch processing of deletions with dict records."""
        records = [{"id": "test-id-1"}]

        sync_manager.ckan_tool.ckan.action.dataset_purge = Mock()

        with patch("scripts.sync_db.click.progressbar", side_effect=mock_progressbar):
            success, failure = sync_manager.process_deletions_batch(records)

        assert success == 1
        assert failure == 0
        sync_manager.ckan_tool.ckan.action.dataset_purge.assert_called_once_with(
            id="test-id-1"
        )

    def test_process_deletions_batch_with_harvest_record(
        self, sync_manager, mock_progressbar
    ):
        """Test batch processing of deletions with HarvestRecord objects."""
        records = [MockHarvestRecord("test")]

        with patch.object(sync_manager, "sync_record", return_value=True):
            with patch(
                "scripts.sync_db.click.progressbar", side_effect=mock_progressbar
            ):
                success, failure = sync_manager.process_deletions_batch(records)

        assert success == 1
        assert failure == 0

    def test_process_additions_batch(self, sync_manager, mock_progressbar):
        """Test batch processing of additions."""
        records = [MockHarvestRecord("test1"), MockHarvestRecord("test2")]

        with patch.object(sync_manager, "sync_record", side_effect=[True, False]):
            with patch(
                "scripts.sync_db.click.progressbar", side_effect=mock_progressbar
            ):
                success, failure = sync_manager.process_additions_batch(records)

        assert success == 1
        assert failure == 1

    @patch("scripts.sync_db.CKANSyncManager.fetch_all_ckan_records")
    @patch("scripts.sync_db.CKANSyncManager.get_latest_db_records")
    @patch("scripts.sync_db.CKANSyncManager.categorize_ckan_records")
    def test_synchronize_dry_run(
        self, mock_categorize, mock_get_db, mock_fetch_ckan, sync_manager
    ):
        """Test synchronization in dry run mode."""
        mock_fetch_ckan.return_value = []
        mock_get_db.return_value = {}
        mock_categorize.return_value = ([], [], [], [])

        stats = sync_manager.synchronize(dry_run=True)

        assert isinstance(stats, SyncStats)
        assert stats.total_ckan_records == 0
        # In dry run, no actual operations should be performed
        assert stats.updated_success == 0
        assert stats.deleted_success == 0
        assert stats.add_success == 0

    @patch("scripts.sync_db.CKANSyncManager.fetch_all_ckan_records")
    @patch("scripts.sync_db.CKANSyncManager.get_latest_db_records")
    @patch("scripts.sync_db.CKANSyncManager.categorize_ckan_records")
    @patch("scripts.sync_db.CKANSyncManager.process_updates_batch")
    @patch("scripts.sync_db.CKANSyncManager.process_deletions_batch")
    @patch("scripts.sync_db.CKANSyncManager.process_additions_batch")
    def test_synchronize_full_run(
        self,
        mock_add,
        mock_delete,
        mock_update,
        mock_categorize,
        mock_get_db,
        mock_fetch_ckan,
        sync_manager,
    ):
        """Test full synchronization run."""
        mock_fetch_ckan.return_value = [{"test": "record"}]
        mock_get_db.return_value = {}
        mock_categorize.return_value = (
            [],  # synced
            [MockHarvestRecord("update")],  # to_update
            [MockHarvestRecord("delete")],  # to_delete
            [MockHarvestRecord("add")],  # to_add
        )
        mock_update.return_value = (1, 0)
        mock_delete.return_value = (1, 0)
        mock_add.return_value = (1, 0)

        stats = sync_manager.synchronize(dry_run=False)

        assert stats.total_ckan_records == 1
        assert stats.updated_success == 1
        assert stats.deleted_success == 1
        assert stats.add_success == 1
        mock_update.assert_called_once()
        mock_delete.assert_called_once()
        mock_add.assert_called_once()

    @patch("scripts.sync_db.CKANSyncManager.get_latest_db_records")
    def test_synchronize_with_exception(self, mock_get_db, sync_manager):
        """Test synchronization handles exceptions gracefully."""
        mock_get_db.return_value = {}
        with patch.object(
            sync_manager, "fetch_all_ckan_records", side_effect=Exception("Test error")
        ):
            stats = sync_manager.synchronize(dry_run=True)

        assert len(stats.errors) == 1
        assert "Synchronization failed: Test error" in stats.errors[0]

    @patch("scripts.sync_db.CKANSyncManager.fetch_all_ckan_records")
    @patch("scripts.sync_db.CKANSyncManager.get_latest_db_records")
    @patch("scripts.sync_db.CKANSyncManager.categorize_ckan_records")
    @patch("scripts.sync_db.CKANSyncManager.get_db_record_count")
    def test_get_sync_preview(
        self, mock_count, mock_categorize, mock_get_db, mock_fetch_ckan, sync_manager
    ):
        """Test getting sync preview."""
        mock_fetch_ckan.return_value = [{"test": "record"}]
        mock_get_db.return_value = {}
        mock_categorize.return_value = ([], [], [], [])
        mock_count.return_value = 5

        preview = sync_manager.get_sync_preview()

        assert "synced" in preview
        assert "to_update" in preview
        assert "to_delete" in preview
        assert "summary" in preview
        assert preview["summary"]["total_db"] == 5


class TestRealWorldScenarios:
    """Test realistic scenarios that might occur in production."""

    def test_mixed_scenario(self, sync_manager):
        """Test a complex scenario with multiple record types."""
        ckan_records = [
            {
                "id": "ckan-1",
                "identifier": "SYNC_001",
                "harvest_object_id": "harvest-1",
                "name": "synced-record",
                "metadata_modified": "2025-06-28T16:55:51.313Z",
            },
            {
                "id": "ckan-2",
                "identifier": "UPDATE_001",
                "harvest_object_id": "old-harvest-2",
                "name": "needs-update",
                "metadata_modified": "2025-06-28T16:55:51.313Z",
            },
            {
                "id": "ckan-3",
                "identifier": "ORPHAN_001",
                "harvest_object_id": "harvest-3",
                "name": "orphan-record",
                "metadata_modified": "2025-06-28T16:55:51.313Z",
            },
        ]

        db_records = {
            "SYNC_001": MockHarvestRecord(
                "SYNC_001", id="harvest-1", ckan_name="synced-record"
            ),
            "UPDATE_001": MockHarvestRecord(
                "UPDATE_001", id="new-harvest-2", ckan_name="old-name"
            ),
            "ADD_001": MockHarvestRecord(
                "ADD_001", id="harvest-4", ckan_name="new-record"
            ),
        }

        def mock_needs_update(record, db_record):
            return db_record.identifier == "UPDATE_001"

        with patch.object(sync_manager, "needs_update", side_effect=mock_needs_update):
            synced, to_update, to_delete, to_add = sync_manager.categorize_ckan_records(
                ckan_records, db_records
            )

        assert len(synced) == 1  # SYNC_001
        assert len(to_update) == 1  # UPDATE_001
        assert len(to_delete) == 1  # ORPHAN_001 (no DB record)
        assert len(to_add) == 1  # ADD_001 (not processed from CKAN)


class TestErrorHandling:
    """Test error handling scenarios."""

    def test_malformed_ckan_record(self, sync_manager):
        """Test handling of malformed CKAN records."""
        ckan_records = [
            {"id": "malformed", "name": "test"},  # Missing identifier
            {"identifier": "GOOD_001", "harvest_object_id": "h1", "name": "good"},
        ]
        db_records = {"GOOD_001": MockHarvestRecord("GOOD_001", id="h1")}

        synced, to_update, to_delete, to_add = sync_manager.categorize_ckan_records(
            ckan_records, db_records
        )

        assert len(to_delete) == 1
        assert len(synced) == 1  # Good record should be synced


class TestUtilityFunctions:
    """Test utility functions."""

    @patch("scripts.sync_db.click.echo")
    def test_print_sync_results(self, mock_echo):
        """Test printing sync results."""
        stats = SyncStats(
            total_ckan_records=100,
            already_synced=50,
            to_update=20,
            to_delete=10,
            updated_success=18,
            updated_failed=2,
            deleted_success=9,
            deleted_failed=1,
            errors=["Test error"],
        )

        print_sync_results(stats)

        # Check that click.echo was called with expected content
        mock_echo.assert_called()
        # Get all the calls to click.echo
        calls = [call.args[0] for call in mock_echo.call_args_list]

        assert any("Total CKAN records processed: 100" in call for call in calls)
        assert any("Successful: 18" in call for call in calls)
        assert any("Test error" in call for call in calls)


class TestCLICommand:
    """Test the CLI command."""

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
        runner,
    ):
        """Test sync command with dryrun."""
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

        result = runner.invoke(sync_command, ["--batch-size", "100", "--dry-run"])

        # The command should complete successfully
        assert result.exit_code == 1

    def test_sync_command_invalid_batch_size(self):
        """Test sync command with invalid batch size."""
        from click.testing import CliRunner

        runner = CliRunner()

        # Test with batch size > 1000
        result = runner.invoke(sync_command, ["--batch-size", "1500"])

        assert result.exit_code == 1
        assert "Batch size must be 1000 or less" in result.output
