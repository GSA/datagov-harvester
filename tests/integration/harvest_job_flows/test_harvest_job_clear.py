from unittest.mock import patch

from harvester.harvest import HarvestSource, ckan_sync_tool, harvest_job_starter


class TestHarvestJobClear:
    @patch("harvester.harvest.ckan_sync_tool.ckan")
    def test_harvest_job_clear(
        self,
        CKANMock,
        interface,
        organization_data,
        source_data_dcatus,
    ):
        CKANMock.action.package_create.return_value = {"id": 1234}
        CKANMock.action.dataset_purge.return_value = {"ok"}
        CKANMock.action.package_update = "ok"

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus["id"],
            }
        )

        job_id = harvest_job.id
        job_type = harvest_job.job_type  # harvest
        harvest_job_starter(job_id, job_type)

        harvest_job = interface.get_harvest_job(job_id)
        assert harvest_job.status == "complete"
        assert harvest_job.records_added == 7
        # - 1 ckan call from close_conection
        assert len(CKANMock.method_calls) - 1 == harvest_job.records_added
        records_from_db = interface.get_latest_harvest_records_by_source(
            source_data_dcatus["id"]
        )

        assert len(records_from_db) == 7

        # now run a clear
        harvest_job = interface.add_harvest_job(
            {
                "status": "new",
                "harvest_source_id": source_data_dcatus["id"],
                "job_type": "clear",
            }
        )

        job_id = harvest_job.id
        job_type = harvest_job.job_type  # clear
        harvest_job_starter(job_id, job_type)

        harvest_job = interface.get_harvest_job(job_id)
        assert harvest_job.status == "complete"
        assert harvest_job.records_deleted == 7
        # 9 = 7 package creates + 1 close connection on harvest
        # + 1 close connection on clear
        assert len(CKANMock.method_calls) - 9 == harvest_job.records_deleted
        records_from_db = interface.get_latest_harvest_records_by_source(
            source_data_dcatus["id"]
        )
        assert len(records_from_db) == 0

    @patch("harvester.harvest.Record.sync")
    def test_harvest_job_clear_not_synced_with_ckan(
        self,
        record_mock,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        record_data_dcatus,
    ):
        record_mock.return_value = False

        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        job = interface.add_harvest_job(job_data_dcatus)
        interface.add_harvest_record(record_data_dcatus[0])

        assert len(interface.pget_harvest_records()) == 1

        harvest_source = HarvestSource(job.id, "clear")
        harvest_source.run_full_harvest()
        harvest_source.report()

        # 1 (original successful create record) + 1 (delete before ckan action)
        assert len(interface.pget_harvest_records()) == 2

        # running clear via "harvest_job_starter" closes the db session
        # preventing access to the data below
        assert job.status == "error"
        assert len(job.errors) == 1
        assert job.errors[0].message == "Test Source failed to clear completely"

        harvest_source.db_interface.close()
        ckan_sync_tool.close_conection()
