from harvester.transform import transform
from harvester.extract import traverse_waf, download_waf
from unittest.mock import patch


@patch("harvester.transform.transform")
def test_transform(mock_transform):
    """tests transform"""

    files = traverse_waf("http://localhost:80", filters=["../", "dcatus/"])
    downloaded_files = download_waf(files)

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
        transform_response = transform(data)
        assert transform_response["transformed_data"] is not None
