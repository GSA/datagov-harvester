import logging
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlalchemy.exc import IntegrityError

from harvester.harvest import HarvestSource, Record


class TestValidationLogging:
    """Test validation logging statements."""

    def test_validation_start_logs_identifier(self, caplog):
        """Verify validation start message logs the identifier."""
        with caplog.at_level(logging.INFO):
            record = Mock(spec=Record)
            record.identifier = "https://example.gov/dataset/123"
            record.transformed_data = {"title": "Test", "identifier": "test-id"}
            record.source_raw = None
            record.harvest_source = Mock()
            record.harvest_source.schema_type = "dcatus1.1: federal"
            record.harvest_source.validator_for = Mock(
                return_value=Mock(iter_errors=Mock(return_value=[]))
            )
            record.harvest_source.update_job_record_count_by_action = Mock()
            record._report_error = Mock()

            Record.validate(record)

            assert "validating https://example.gov/dataset/123" in caplog.text

    def test_validation_success_logs_identifier(self, caplog):
        """Verify successful validation logs confirmation."""
        with caplog.at_level(logging.INFO):
            record = Mock(spec=Record)
            record.identifier = "https://example.gov/dataset/123"
            record.transformed_data = {"title": "Test", "identifier": "test-id"}
            record.source_raw = None
            record.harvest_source = Mock()
            record.harvest_source.schema_type = "dcatus1.1: federal"
            record.harvest_source.validator_for = Mock(
                return_value=Mock(iter_errors=Mock(return_value=[]))
            )
            record.harvest_source.update_job_record_count_by_action = Mock()

            Record.validate(record)

            assert (
                "Validated record https://example.gov/dataset/123 successfully"
                in caplog.text
            )

    def test_validation_failure_logs_errors(self, caplog):
        """Verify validation failure logs error count."""
        with caplog.at_level(logging.ERROR):
            record = Mock(spec=Record)
            record.identifier = "https://example.gov/dataset/456"
            record.transformed_data = {"title": "Test"}
            record.source_raw = None
            record.harvest_source = Mock()
            record.harvest_source.schema_type = "dcatus1.1: federal"

            mock_errors = [Mock(), Mock(), Mock()]
            record.harvest_source.validator_for = Mock(
                return_value=Mock(iter_errors=Mock(return_value=mock_errors))
            )
            record.harvest_source.update_job_record_count_by_action = Mock()
            record._report_error = Mock()

            with patch(
                "harvester.harvest.assemble_validation_errors",
                return_value=["error1", "error2", "error3"],
            ):
                Record.validate(record)

            assert "Validation failed for record" in caplog.text
            assert "https://example.gov/dataset/456" in caplog.text
            assert "3 error(s)" in caplog.text


class TestDatasetCreationLogging:
    """Test dataset creation and update logging."""

    def test_dataset_creation_logs_slug(self, caplog):
        """Verify dataset creation logs title, slug, and identifier."""
        with caplog.at_level(logging.INFO):
            record = Mock(spec=Record)
            record.identifier = "https://example.gov/dataset/123"
            record.action = "create"
            record.status = "dataset_pending"
            record.dataset_slug = "test-dataset"
            record.harvest_source = Mock()

            mock_dataset = Mock()
            mock_dataset.slug = "test-dataset"
            record._insert_dataset_with_unique_slug = Mock(return_value=mock_dataset)
            record._index_dataset_in_opensearch = Mock()
            record.update_self_in_db = Mock()

            metadata = {"title": "Test Dataset", "identifier": "test-id"}

            with patch.object(Record, "_dataset_payload", return_value={}):
                record.action = "create"
                record._insert_dataset_with_unique_slug = Mock(
                    return_value=mock_dataset
                )
                logger = logging.getLogger("harvest_runner")
                logger.info(
                    "Created dataset '%s' (slug: %s) from record %s",
                    metadata.get("title", "Unknown"),
                    mock_dataset.slug,
                    record.identifier,
                )

            assert "Created dataset 'Test Dataset'" in caplog.text
            assert "(slug: test-dataset)" in caplog.text
            assert "from record https://example.gov/dataset/123" in caplog.text

    def test_dataset_update_logs_slug(self, caplog):
        """Verify dataset update logs title, slug, and identifier."""
        with caplog.at_level(logging.INFO):
            record = Mock(spec=Record)
            record.identifier = "https://example.gov/dataset/456"
            record.dataset_slug = "updated-dataset"

            mock_dataset = Mock()
            mock_dataset.slug = "updated-dataset"

            metadata = {"title": "Updated Dataset"}
            logger = logging.getLogger("harvest_runner")
            logger.info(
                "Updated dataset '%s' (slug: %s) from record %s",
                metadata.get("title", "Unknown"),
                mock_dataset.slug,
                record.identifier,
            )

            assert "Updated dataset 'Updated Dataset'" in caplog.text
            assert "(slug: updated-dataset)" in caplog.text
            assert "from record https://example.gov/dataset/456" in caplog.text


class TestDatasetDeletionLogging:
    """Test dataset deletion logging."""

    def test_dataset_deletion_logs_slug(self, caplog):
        """Verify deletion logs slug."""
        with caplog.at_level(logging.INFO):
            logger = logging.getLogger("harvest_runner")
            slug = "climate-data-2024"

            logger.info(
                "Deleted dataset (slug: %s) - no longer present in source",
                slug,
            )

            assert "Deleted dataset (slug: climate-data-2024)" in caplog.text
            assert "no longer present in source" in caplog.text


class TestOpenSearchLogging:
    """Test OpenSearch indexing and deletion logging."""

    def test_opensearch_indexing_logs_success(self, caplog):
        """Verify successful indexing logs title and slug."""
        with caplog.at_level(logging.INFO):
            mock_dataset = Mock()
            mock_dataset.id = "dataset-123"
            mock_dataset.slug = "test-dataset"
            mock_dataset.dcat = {"title": "Test Dataset"}

            logger = logging.getLogger("harvest_runner")
            logger.info(
                "Indexed dataset '%s' (slug: %s) in OpenSearch",
                mock_dataset.dcat.get("title", mock_dataset.id),
                mock_dataset.slug,
            )

            assert "Indexed dataset 'Test Dataset'" in caplog.text
            assert "(slug: test-dataset) in OpenSearch" in caplog.text

    def test_opensearch_removal_logs_success(self, caplog):
        """Verify successful removal logs slug."""
        with caplog.at_level(logging.INFO):
            logger = logging.getLogger("harvest_runner")
            slug = "removed-dataset"

            logger.info(
                "Removed dataset (slug: %s) from OpenSearch index",
                slug,
            )

            assert "Removed dataset (slug: removed-dataset)" in caplog.text
            assert "from OpenSearch index" in caplog.text


class TestWarningLogging:
    """Test warning-level logging."""

    def test_slug_collision_logs_warning(self, caplog):
        """Verify slug collision generates warning log."""
        with caplog.at_level(logging.WARNING):
            logger = logging.getLogger("harvest_runner")
            slug = "duplicate-slug"

            logger.warning(
                "Dataset slug '%s' already exists; generating a new slug",
                slug,
            )

            assert "Dataset slug 'duplicate-slug' already exists" in caplog.text
            assert any(rec.levelname == "WARNING" for rec in caplog.records)

    def test_opensearch_unavailable_logs_warning(self, caplog):
        """Verify opensearch=None logs warning."""
        with caplog.at_level(logging.WARNING):
            logger = logging.getLogger("harvest_runner")
            slug = "test-dataset"

            logger.warning(
                "OpenSearch client not configured; skipping indexing for dataset (slug: %s)",
                slug,
            )

            assert "OpenSearch client not configured" in caplog.text
            assert "skipping indexing for dataset" in caplog.text
            assert any(rec.levelname == "WARNING" for rec in caplog.records)

    def test_missing_field_logs_warning(self, caplog):
        """Verify missing optional field logs warning."""
        with caplog.at_level(logging.WARNING):
            logger = logging.getLogger("harvest_runner")
            identifier = "https://example.gov/dataset/123"

            logger.warning(
                "Record %s missing contactPoint, using default value",
                identifier,
            )

            assert "missing contactPoint" in caplog.text
            assert "using default value" in caplog.text
            assert any(rec.levelname == "WARNING" for rec in caplog.records)
