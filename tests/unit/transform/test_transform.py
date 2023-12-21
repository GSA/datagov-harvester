# from harvester.transform import transform
import harvester
from unittest.mock import patch


@patch("harvester.transform")
def test_transform(mock_transform):
    """tests transform"""

    files = harvester.traverse_waf("http://localhost:80", filters=["../", "dcatus/"])
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
