# from harvester.transform import transform
from unittest.mock import patch

import harvester


@patch("harvester.transform")
def test_transform(mock_transform):
    """tests transform"""

    source = harvester.HarvestSource(url="http://localhost:80", source_type="waf")
    files = harvester.traverse_waf(source.url, filters=["../", "dcatus/"])
    downloaded_files = harvester.download_waf(files)

    for file in downloaded_files:
        data = {
            "file": file["content"],
            "reader": "fgdc",
            "writer": "iso19115_3",
        }

        mock_transform.return_value = {
            **data.copy(),
            **{"transformed_data": "mock_xml_data"},
        }
        transform_response = harvester.transform(data)
        assert transform_response["transformed_data"] is not None
