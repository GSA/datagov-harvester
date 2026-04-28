from unittest.mock import MagicMock, patch
from scripts.add_missing_datasets import main
from database.models import HarvestRecord, Organization, Dataset
from datetime import datetime


class TestAddMissingDatasetsScript:
    def test_main_with_mocked_harvest_records(self):
        harvest_record = MagicMock(spec=HarvestRecord)
        harvest_record.id = 101
        harvest_record.source_transform = None
        harvest_record.source_raw = '{ "title": "Aquarius Official Release Level 3 Sea Surface Spiciness Standard Mapped Image Ascending Seasonal Data V5.0", "spatial": "16.0, 5.0, 12.0, 67.0"}'
        harvest_record.harvest_source_id = 1
        harvest_record.date_finished = datetime.now()

        harvest_record1 = MagicMock(spec=HarvestRecord)
        harvest_record1.id = 102
        harvest_record1.source_transform = None
        harvest_record1.source_raw = "<xml>something</xml>"
        harvest_record1.harvest_source_id = 1
        harvest_record1.date_finished = datetime.now()

        org = MagicMock(spec=Organization)
        org.id = 123

        fake_interface = MagicMock()

        mock_dataset = MagicMock(spec=Dataset)
        mock_dataset.id = 123
        mock_dataset.slug = "my-dataset-slug"

        fake_interface.get_dataset_by_slug = MagicMock(side_effect=[mock_dataset, None])

        with (
            patch(
                "scripts.add_missing_datasets.HarvesterDBInterface",
                return_value=fake_interface,
            ),
            patch(
                "scripts.add_missing_datasets.get_missing_datasets",
                return_value=(2, [harvest_record, harvest_record1]),
            ),
            patch(
                "scripts.add_missing_datasets.get_org_from_record",
                return_value=org,
            ),
        ):
            main()
