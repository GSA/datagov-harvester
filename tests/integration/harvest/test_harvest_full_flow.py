from unittest.mock import patch

from harvester.harvest import HarvestSource


class TestHarvestFullFlow:
    @patch("harvester.harvest.ckan")
    def test_harvest_single_record_created(
        self,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus_single_record,
    ):
        CKANMock.return_value.action = "ok"
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus_single_record)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus_single_record["id"],
            }
        )
        job_id = harvest_job.id
        harvest_source = HarvestSource(job_id)
        harvest_source.get_record_changes()
        harvest_source.write_compare_to_db()
        harvest_source.synchronize_records()
        harvest_source.report()
        harvest_job = interface.get_harvest_job(job_id)
        assert harvest_job.status == "complete"
        assert harvest_job.records_added == len(harvest_source.external_records)

    @patch("harvester.harvest.RemoteCKAN")
    @patch("harvester.harvest.download_file")
    def test_harvest_record_errors_reported(
        self,
        download_file_mock,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        single_internal_record,
    ):
        CKANMock.return_value.action.dataset_purge.side_effect = Exception()
        download_file_mock.return_value = dict({"dataset": []})
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(job_data_dcatus)

        interface.add_harvest_record(single_internal_record)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.get_record_changes()
        harvest_source.write_compare_to_db()
        harvest_source.synchronize_records()
        harvest_source.report()

        interface_errors = interface.get_harvest_record_errors_by_record(
            harvest_source.internal_records_lookup_table[
                single_internal_record["identifier"]
            ]
        )

        job_errors = [
            error for record in harvest_job.records for error in record.errors
        ]
        assert harvest_job.status == "complete"
        assert len(interface_errors) == harvest_job.records_errored
        assert len(interface_errors) == len(job_errors)
        assert interface_errors[0].harvest_record_id == job_errors[0].harvest_record_id
