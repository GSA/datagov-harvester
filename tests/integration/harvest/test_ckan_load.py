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
        # add the necessary records to satisfy FKs
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        interface.add_harvest_job(job_data_dcatus)

        # prefill with records
        for record in internal_compare_data["records"]:
            data = {
                "identifier": record["identifier"],
                "harvest_job_id": internal_compare_data["job_id"],
                "harvest_source_id": internal_compare_data["harvest_source_id"],
                "source_hash": dataset_to_hash(sort_dataset(record)),
                "status": "success",
                "action": "create",
            }
            interface.add_harvest_record(data)

        harvest_source = HarvestSource(internal_compare_data["job_id"])
        harvest_source.get_record_changes()
        harvest_source.write_compare_to_db()
        harvest_source.synchronize_records()

        created = sum(
            r.action == "create" for rid, r in harvest_source.external_records.items()
        )
        updated = sum(
            r.action == "update" for rid, r in harvest_source.external_records.items()
        )

        deleted = sum(
            r.action == "delete" for rid, r in harvest_source.external_records.items()
        )
        succeeded = sum(
            r.status == "success" for rid, r in harvest_source.external_records.items()
        )
        errored = sum(
            r.action == "error" for rid, r in harvest_source.external_records.items()
        )

        assert created == 6
        assert updated == 1
        assert deleted == 1
        assert succeeded == 8
        assert errored == 0

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

        records = [
            (
                {
                    "identifier": "cftc-dc1",
                    "harvest_job_id": job_data_dcatus["id"],
                    "harvest_source_id": job_data_dcatus["harvest_source_id"],
                }
            )
        ]
        interface.add_harvest_records(records)
        harvest_source.get_record_changes()
        harvest_source.write_compare_to_db()
        record_id = harvest_source.internal_records_lookup_table["cftc-dc1"]

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
                {"key": "harvest_object_id", "value": record_id},
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
                # {
                #     "key": "dcat_metadata",
                #     "value": "{'accessLevel': 'public', 'bureauCode': ['339:00'], 'contactPoint': {'fn': 'Harold W. Hild', 'hasEmail': 'mailto:hhild@CFTC.GOV'}, 'describedBy': 'https://www.cftc.gov/MarketReports/CommitmentsofTraders/ExplanatoryNotes/index.htm', 'description': \"COT reports provide a breakdown of each Tuesday's open interest for futures and options on futures market in which 20 or more traders hold positions equal to or above the reporting levels established by CFTC\", 'distribution': [{'accessURL': 'https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm'}], 'identifier': 'cftc-dc1', 'keyword': ['commitment of traders', 'cot', 'open interest'], 'modified': 'R/P1W', 'programCode': ['000:000'], 'publisher': {'name': 'U.S. Commodity Futures Trading Commission', 'subOrganizationOf': {'name': 'U.S. Government'}}, 'title': 'Commitment of Traders'}",
                # },
                # {"key": "harvest_source_name", "value": "test_harvest_source_name"},
                {"key": "identifier", "value": "cftc-dc1"},
            ],
        }

        test_record = harvest_source.external_records["cftc-dc1"]
        test_record.ckanify_dcatus()
        assert DeepDiff(test_record.ckanified_metadata, expected_result) == {}


#     # TODO: add sort test
