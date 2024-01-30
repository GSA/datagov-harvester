from harvester.harvest import HarvestSource
from harvester.utils import S3Handler
from unittest.mock import patch


class TestS3Load:
    @patch.object(HarvestSource, "get_ckan_records")
    def test_upload_harvest_source(
        self, get_ckan_records_mock, dcatus_compare_config, ckan_compare
    ):
        get_ckan_records_mock.return_value = ckan_compare

        harvest_source = HarvestSource(**dcatus_compare_config)
        harvest_source.get_record_changes()

        s3_handler = S3Handler()
        s3_path = harvest_source.upload_to_s3(s3_handler)

        assert s3_path is not False

        s3_handler.delete_object(s3_path)

    @patch.object(HarvestSource, "get_ckan_records")
    def test_upload_record(
        self, get_ckan_records_mock, dcatus_compare_config, ckan_compare
    ):
        get_ckan_records_mock.return_value = ckan_compare

        harvest_source = HarvestSource(**dcatus_compare_config)
        harvest_source.get_record_changes()
        test_record = harvest_source.records["cftc-dc7"]

        s3_handler = S3Handler()
        s3_path = test_record.upload_to_s3(s3_handler)

        assert s3_path is not False

        s3_handler.delete_object(s3_path)
