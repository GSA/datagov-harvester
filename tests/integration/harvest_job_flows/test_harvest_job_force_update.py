from datetime import datetime
import json
from unittest.mock import Mock, patch

from database.models import Dataset
from harvester.harvest import HarvestSource, harvest_job_starter


class TestHarvestJobSync:
    def test_harvest_job_force_update(
        self,
        interface,
        organization_data,
        source_data_dcatus,
    ):
        """
        Force update exists to update jobs in case
        of code changes when we want to update all datasets,
        not just those that have changed
        """
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)

        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus["id"],
            }
        )

        job_id = harvest_job.id
        job_type = harvest_job.job_type
        assert job_type == "harvest"
        harvest_job_starter(job_id, job_type)

        harvest_job = interface.get_harvest_job(job_id)
        job_err = interface.get_harvest_job_errors_by_job(job_id)
        record_err = interface.get_harvest_record_errors_by_job(job_id)

        assert len(job_err) == 0
        assert len(record_err) == 0

        assert harvest_job.status == "complete"
        assert harvest_job.records_added == 7
        assert harvest_job.records_deleted == 0
        assert harvest_job.records_errored == 0
        assert harvest_job.records_ignored == 0
        assert harvest_job.records_total == 7
        assert harvest_job.records_updated == 0
        assert harvest_job.records_validated == 7

        datasets_initial = interface.db.query(Dataset).all()
        assert len(datasets_initial) == 7
        initial_harvest_record_ids = {
            dataset.slug: dataset.harvest_record_id for dataset in datasets_initial
        }

        ## create a second force_harvest to pickup sync
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus["id"],
                "job_type": "force_harvest",
            }
        )

        job_id = harvest_job.id
        job_type = harvest_job.job_type
        assert job_type == "force_harvest"
        harvest_job_starter(job_id, job_type)

        harvest_job = interface.get_harvest_job(job_id)

        # assert all records are resynced.
        assert len(job_err) == 0
        assert len(record_err) == 0

        assert harvest_job.status == "complete"
        assert harvest_job.records_added == 0
        assert harvest_job.records_deleted == 0
        assert harvest_job.records_errored == 0
        assert harvest_job.records_ignored == 0
        assert harvest_job.records_total == 7
        assert harvest_job.records_updated == 7
        assert harvest_job.records_validated == 7

        datasets_after = interface.db.query(Dataset).all()
        assert len(datasets_after) == 7
        updated_harvest_record_ids = {
            dataset.slug: dataset.harvest_record_id for dataset in datasets_after
        }
        assert set(initial_harvest_record_ids) == set(updated_harvest_record_ids)
        for slug, record_id in initial_harvest_record_ids.items():
            assert updated_harvest_record_ids[slug] != record_id

    def test_waf_force_harvest_skips_datetime_filter(
        self,
        interface,
        organization_data,
        source_data_waf_iso19115_2,
        iso19115_2_transform,
    ):
        """
        Force harvest for WAF sources should skip filter_waf_files_by_datetime
        to ensure all records are updated, not just those with newer modified dates.
        """
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_waf_iso19115_2)

        # Mock external dependencies
        mock_waf_files = [
            {
                "identifier": f"{source_data_waf_iso19115_2['url']}file1.xml",
                "modified_date": datetime(2024, 1, 1),
            },
            {
                "identifier": f"{source_data_waf_iso19115_2['url']}file2.xml",
                "modified_date": datetime(2024, 1, 2),
            },
        ]

        valid_xml = """<?xml version="1.0" encoding="UTF-8"?>
<gmi:MI_Metadata xmlns:gmi="http://www.isotc211.org/2005/gmi">
    <gmd:fileIdentifier>
        <gco:CharacterString>test-file</gco:CharacterString>
    </gmd:fileIdentifier>
</gmi:MI_Metadata>"""

        # Mock MDTranslator response
        mock_mdt_response = Mock()
        mock_mdt_response.status_code = 200
        mock_mdt_response.json.return_value = {
            "writerOutput": json.dumps(iso19115_2_transform),
            "readerStructureMessages": [],
            "readerValidationMessages": [],
        }

        with patch("harvester.harvest.traverse_waf", return_value=mock_waf_files), \
             patch("harvester.harvest.download_file", return_value=valid_xml), \
             patch("harvester.harvest.requests.post", return_value=mock_mdt_response):

            # Initial harvest
            harvest_job = interface.add_harvest_job(
                {
                    "status": "new",
                    "harvest_source_id": source_data_waf_iso19115_2["id"],
                }
            )

            job_id = harvest_job.id
            job_type = harvest_job.job_type
            assert job_type == "harvest"
            harvest_job_starter(job_id, job_type)

            harvest_job = interface.get_harvest_job(job_id)
            job_err = interface.get_harvest_job_errors_by_job(job_id)
            record_err = interface.get_harvest_record_errors_by_job(job_id)

            assert len(job_err) == 0
            assert len(record_err) == 0
            assert harvest_job.status == "complete"

            initial_records_added = harvest_job.records_added
            assert initial_records_added > 0

            datasets_initial = interface.db.query(Dataset).all()
            assert len(datasets_initial) == initial_records_added
            initial_harvest_record_ids = {
                dataset.slug: dataset.harvest_record_id for dataset in datasets_initial
            }

            # Force harvest - should skip filter_waf_files_by_datetime
            with patch.object(
                HarvestSource, "filter_waf_files_by_datetime"
            ) as mock_filter:
                harvest_job = interface.add_harvest_job(
                    {
                        "status": "new",
                        "harvest_source_id": source_data_waf_iso19115_2["id"],
                        "job_type": "force_harvest",
                    }
                )

                job_id = harvest_job.id
                job_type = harvest_job.job_type
                assert job_type == "force_harvest"
                harvest_job_starter(job_id, job_type)

                # Verify filter was NOT called
                mock_filter.assert_not_called()

            harvest_job = interface.get_harvest_job(job_id)

            # Assert all records are resynced
            assert len(job_err) == 0
            assert len(record_err) == 0
            assert harvest_job.status == "complete"
            assert harvest_job.records_added == 0
            assert harvest_job.records_deleted == 0
            assert harvest_job.records_errored == 0
            assert harvest_job.records_ignored == 0
            assert harvest_job.records_total == initial_records_added
            assert harvest_job.records_updated == initial_records_added
            assert harvest_job.records_validated == initial_records_added

            datasets_after = interface.db.query(Dataset).all()
            assert len(datasets_after) == initial_records_added
            updated_harvest_record_ids = {
                dataset.slug: dataset.harvest_record_id for dataset in datasets_after
            }
            assert set(initial_harvest_record_ids) == set(updated_harvest_record_ids)
            for slug, record_id in initial_harvest_record_ids.items():
                assert updated_harvest_record_ids[slug] != record_id