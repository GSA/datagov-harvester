from harvester.harvest import HarvestSource
from unittest.mock import patch


# ruff: noqa: E501
class TestExtractCKAN:
    @patch.object(HarvestSource, "get_ckan_records")
    def test_retrieve_from_ckan_and_hash(
        self, get_ckan_records_mock, dcatus_config, ckan_compare
    ):
        get_ckan_records_mock.return_value = ckan_compare

        harvest_source = HarvestSource(**dcatus_config)
        harvest_source.get_ckan_records_as_id_hash()

        expected_result = {
            "cftc-dc5": "678c475a7dba803f04526afa092b13878dde44b19824a73c044fe6b6a3e0d644",
            "cftc-dc4": "bee962dd27d5da4ea68e97f25a602ecf5c7c4f6dc7f2a13a802162e051a93c2f",
            "cftc-dc6": "0e714b73c9d822994fa86baaf32815a2fb83bcbd8d1a78c918fd21f2a1a3cdaf",
            "cftc-dc7": "bac96fcf129ae2cb6145e342018ac3230eeb9fd96d38d23c0d652af0ade9c743",
            "cftc-dc2": "6c872b3662928f8506feedb1e8c3615c71ce7e124abeff08df750f71cf02a670",
            "cftc-dc1": "8e4acdb04e627ae65f0721afebd9aabdbd59a8c01b129e937bfa2b31f1034a19",
            "cftc-dc3": "d3a2b11fb451b1366d25f173a5a49c60c723959d3e83531cff80b4e3562c079b",
        }

        assert harvest_source.ckan_records == expected_result
