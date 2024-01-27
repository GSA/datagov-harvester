from harvester.harvest import HarvestSource, Record
from unittest.mock import patch
from deepdiff import DeepDiff

# ruff: noqa: E501


class TestCKANLoad:
    def delete_mock(self):
        self.status = "deleted"

    def update_mock(self):
        self.status = "updated"

    def create_mock(self):
        self.status = "created"

    @patch.object(Record, "create_record", create_mock)
    @patch.object(Record, "update_record", update_mock)
    @patch.object(Record, "delete_record", delete_mock)
    @patch.object(HarvestSource, "get_ckan_records")
    def test_sync(self, get_ckan_records_mock, dcatus_compare_config, ckan_compare):
        harvest_source = HarvestSource(**dcatus_compare_config)
        get_ckan_records_mock.return_value = ckan_compare

        harvest_source.get_record_changes()
        harvest_source.synchronize_records()

        assert (
            sum(r.status == "created" for rid, r in harvest_source.records.items()) == 1
        )
        assert (
            sum(r.status == "updated" for rid, r in harvest_source.records.items()) == 3
        )
        assert (
            sum(r.status == "deleted" for rid, r in harvest_source.records.items()) == 1
        )

    def test_ckanify_dcatus(self, dcatus_config):
        harvest_source = HarvestSource(**dcatus_config)
        harvest_source.get_harvest_records_as_id_hash()

        expected_result = {
            "name": "commitmentoftraders",
            "owner_org": "test",
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
                {
                    "key": "dcat_metadata",
                    "value": "{'accessLevel': 'public', 'bureauCode': ['339:00'], 'contactPoint': {'fn': 'Harold W. Hild', 'hasEmail': 'mailto:hhild@CFTC.GOV'}, 'describedBy': 'https://www.cftc.gov/MarketReports/CommitmentsofTraders/ExplanatoryNotes/index.htm', 'description': \"COT reports provide a breakdown of each Tuesday's open interest for futures and options on futures market in which 20 or more traders hold positions equal to or above the reporting levels established by CFTC\", 'distribution': [{'accessURL': 'https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm'}], 'identifier': 'cftc-dc1', 'keyword': ['commitment of traders', 'cot', 'open interest'], 'modified': 'R/P1W', 'programCode': ['000:000'], 'publisher': {'name': 'U.S. Commodity Futures Trading Commission', 'subOrganizationOf': {'name': 'U.S. Government'}}, 'title': 'Commitment of Traders'}",
                },
                {"key": "harvest_source_name", "value": "test_harvest_source_name"},
                {"key": "identifier", "value": "cftc-dc1"},
            ],
        }

        test_record = harvest_source.records["cftc-dc1"]
        test_record.ckanify_dcatus()
        assert DeepDiff(test_record.ckanified_metadata, expected_result) == {}


#     # TODO: add sort test
