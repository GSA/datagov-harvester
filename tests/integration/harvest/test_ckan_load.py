from itertools import groupby
from unittest.mock import patch

from deepdiff import DeepDiff

from harvester.harvest import HarvestSource, Record
from harvester.utils.general_utils import dataset_to_hash, sort_dataset

# ruff: noqa: E501


class TestCKANLoad:
    def delete_mock(self):
        pass

    def update_mock(self):
        pass

    def create_mock(self):
        pass

    @patch.object(Record, "create_record", create_mock)
    @patch.object(Record, "update_record", update_mock)
    @patch.object(Record, "delete_record", delete_mock)
    def test_sync(
        self,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        interface,
        internal_compare_data,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        interface.add_harvest_job(job_data_dcatus)

        job_id = job_data_dcatus["id"]
        source_id = source_data_dcatus["id"]

        # prefill with records
        for record in internal_compare_data["records"]:
            data = {
                "identifier": record["identifier"],
                "harvest_job_id": job_id,
                "harvest_source_id": source_id,
                "source_hash": dataset_to_hash(sort_dataset(record)),
                "status": "success",
                "action": "create",
            }
            interface.add_harvest_record(data)

        harvest_source = HarvestSource(job_id)
        harvest_source.extract()
        harvest_source.compare()
        harvest_source.transform()
        harvest_source.validate()
        harvest_source.sync()
        harvest_source.do_report()

        results = {
            "action": {"create": 0, "update": 0, "delete": 0, None: 0},
            "status": {"success": 0, "error": 0, None: 0},
            "validity": {True: 0, False: 0},
        }
        for key, group in groupby(
            harvest_source.records, lambda x: x.action if x.status != "error" else None
        ):
            results["action"][key] = sum(1 for _ in group)

        for key, group in groupby(harvest_source.records, lambda x: x.status):
            results["status"][key] = sum(1 for _ in group)

        assert results["action"]["create"] == 6
        assert harvest_source.report["records_added"] == 6

        assert results["action"]["update"] == 1
        assert harvest_source.report["records_updated"] == 1

        assert results["action"]["delete"] == 1
        assert harvest_source.report["records_deleted"] == 1

        assert results["status"]["error"] == 0
        assert harvest_source.report["records_errored"] == 0

        # NOTE: we don't report this status, but it is not in sync b/c deletes aren't counted correctly
        assert results["status"]["success"] == 8
        assert (
            len(harvest_source.records) - harvest_source.report["records_errored"] == 8
        )

    def test_ckanify_dcatus(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        harvest_job = interface.add_harvest_job(job_data_dcatus)

        harvest_source = HarvestSource(harvest_job.id)
        harvest_source.prepare_external_data()

        record = [
            (
                {
                    "identifier": "cftc-dc1",
                    "harvest_job_id": job_data_dcatus["id"],
                    "harvest_source_id": job_data_dcatus["harvest_source_id"],
                }
            )
        ]
        interface.add_harvest_records(record)
        harvest_source.extract()
        harvest_source.compare()
        test_record = [x for x in harvest_source.records if x.identifier == "cftc-dc1"][
            0
        ]

        expected_result = {
            "name": "commitment-of-traders",
            "owner_org": "d925f84d-955b-4cb7-812f-dcfd6681a18f",
            "identifier": "cftc-dc1",
            "author": None,
            "author_email": None,
            "maintainer": "Harold W. Hild",
            "maintainer_email": "hhild@CFTC.GOV",
            "notes": "COT reports provide a breakdown of each Tuesday's open interest for futures and options on futures market in which 20 or more traders hold positions equal to or above the reporting levels established by CFTC",
            "title": "Commitment of Traders",
            "resources": [
                {
                    "url": "https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm"
                }
            ],
            "tags": [
                {"name": "commitment-of-traders"},
                {"name": "cot"},
                {"name": "open-interest"},
            ],
            "extras": [
                {"key": "resource-type", "value": "Dataset"},
                {"key": "harvest_object_id", "value": test_record.id},
                {"key": "source_datajson_identifier", "value": True},
                {
                    "key": "harvest_source_id",
                    "value": "2f2652de-91df-4c63-8b53-bfced20b276b",
                },
                {"key": "harvest_source_title", "value": "Test Source"},
                {"key": "accessLevel", "value": "public"},
                {"key": "bureauCode", "value": "339:00"},
                {"key": "identifier", "value": "cftc-dc1"},
                {"key": "modified", "value": "R/P1W"},
                {"key": "programCode", "value": "000:000"},
                {
                    "key": "publisher_hierarchy",
                    "value": "U.S. Government > U.S. Commodity Futures Trading Commission",
                },
                {
                    "key": "publisher",
                    "value": "U.S. Commodity Futures Trading Commission",
                },
                {"key": "identifier", "value": "cftc-dc1"},
            ],
        }

        test_record.ckanify_dcatus()
        assert DeepDiff(test_record.ckanified_metadata, expected_result) == {}
