from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from freezegun import freeze_time


@freeze_time("Mar 20th, 2026")
class TestDatasetSlugProtection:
    """
    Tests ensuring the harvest cannot overwrite a dataset slug
    that has been edited by a user via the dataset detail view.
    """

    def _setup_dataset(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        slug,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        job_data_dcatus["harvest_source_id"] = source_data_dcatus["id"]
        interface.add_harvest_job(job_data_dcatus)
        record = interface.add_harvest_record(
            {
                "identifier": slug,
                "harvest_job_id": job_data_dcatus["id"],
                "harvest_source_id": source_data_dcatus["id"],
                "status": "success",
                "action": "create",
                "source_raw": "{}",
            }
        )
        payload = {
            "slug": slug,
            "dcat": {"title": slug},
            "organization_id": organization_data["id"],
            "harvest_source_id": source_data_dcatus["id"],
            "harvest_record_id": record.id,
            "last_harvested_date": datetime.now(timezone.utc),
        }
        dataset = interface.insert_dataset(payload)
        return dataset, record

    def test_upsert_does_not_change_existing_slug(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
    ):
        """
        upsert_dataset must not alter the slug column of an existing row.

        Even when a payload containing the same slug is passed in, the slug
        must remain unchanged after the ON CONFLICT UPDATE path executes.
        """
        original_slug = "slug-protection-no-change"
        dataset, record = self._setup_dataset(
            interface,
            organization_data,
            source_data_dcatus,
            job_data_dcatus,
            original_slug,
        )

        # Re-submit with updated dcat slug in payload is the same
        update_payload = {
            "slug": original_slug,
            "dcat": {"title": "Updated via harvest"},
            "organization_id": organization_data["id"],
            "harvest_source_id": source_data_dcatus["id"],
            "harvest_record_id": record.id,
            "last_harvested_date": datetime.now(timezone.utc),
        }
        result = interface.upsert_dataset(update_payload)

        assert result.slug == original_slug
        assert result.dcat == {"title": "Updated via harvest"}

    def test_user_edited_slug_survives_harvest_upsert(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
    ):
        """
        A user-edited slug must not be overwritten when harvest upserts.
        """
        harvest_slug = "slug-protection-harvest-generated"
        dataset, record = self._setup_dataset(
            interface,
            organization_data,
            source_data_dcatus,
            job_data_dcatus,
            harvest_slug,
        )

        # Simulate user editing the slug via the dataset detail form.
        user_slug = "slug-protection-custom-user"
        mock_client = MagicMock()
        mock_client.index_datasets.return_value = (1, 0, [])
        with patch(
            "harvester.opensearch.client.OpenSearchClient.from_environment",
            return_value=mock_client,
        ):
            interface.update_dataset_slug(dataset.id, user_slug)

        # On the next harvest run the harvester queries the DB for the
        # current slug
        reharvest_payload = {
            "slug": user_slug,
            "dcat": {"title": "Re-harvested Title"},
            "organization_id": organization_data["id"],
            "harvest_source_id": source_data_dcatus["id"],
            "harvest_record_id": record.id,
            "last_harvested_date": datetime.now(timezone.utc),
        }
        result = interface.upsert_dataset(reharvest_payload)

        assert result.slug == user_slug
        assert result.dcat == {"title": "Re-harvested Title"}

    def test_update_dataset_slug_passes_updated_slug_to_opensearch(
        self, interface, slug_protection_dataset
    ):
        """
        Test to confirm that the updated dataset.slug is past to opensearch.
        """
        mock_client = MagicMock()
        mock_client.index_datasets.return_value = (1, 0, [])

        with patch(
            "harvester.opensearch.OpenSearchWriter",
            return_value=mock_client,
        ):
            interface.update_dataset_slug(
                slug_protection_dataset.id, "reindex-slug-check"
            )

        (indexed_datasets,), _ = mock_client.index_datasets.call_args
        assert len(indexed_datasets) == 1
        assert indexed_datasets[0].slug == "reindex-slug-check"

    def test_update_dataset_slug_logs_error_on_opensearch_failure(
        self, interface, slug_protection_dataset, caplog
    ):
        """
        When OpenSearch reports index failures, update_dataset_slug must:
        - still return the updated dataset (DB commit succeeded)
        - return os_synced=False
        - return an error string describing the failure
        - log the error at ERROR level
        """
        mock_client = MagicMock()
        mock_client.index_datasets.return_value = (0, 1, [{"error": "boom"}])

        with patch(
            "harvester.opensearch.OpenSearchWriter",
            return_value=mock_client,
        ):
            with caplog.at_level("ERROR"):
                result, os_synced, os_error = interface.update_dataset_slug(
                    slug_protection_dataset.id, "reindex-error-path"
                )

        assert result is not None
        assert result.slug == "reindex-error-path"
        assert os_synced is False
        assert os_error is not None
        assert "boom" in os_error
        assert any("OpenSearch" in message for message in caplog.messages)

    def test_update_dataset_slug_returns_error_string_on_opensearch_exception(
        self, interface, slug_protection_dataset, caplog
    ):
        """
        When OpenSearch raises an exception, the error string must contain the
        exception message so the caller can surface it to the user.
        """
        with patch(
            "harvester.opensearch.OpenSearchWriter",
            side_effect=RuntimeError("OPENSEARCH_HOST is not set"),
        ):
            with caplog.at_level("ERROR"):
                result, os_synced, os_error = interface.update_dataset_slug(
                    slug_protection_dataset.id, "reindex-exception-path"
                )

        assert result is not None
        assert result.slug == "reindex-exception-path"
        assert os_synced is False
        assert os_error is not None
        assert "OPENSEARCH_HOST is not set" in os_error
