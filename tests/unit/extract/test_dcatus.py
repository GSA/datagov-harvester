from harvester import HarvestSource
from harvester.extract import download_dcatus_catalog


def test_extract(dcat_example: HarvestSource):
    """download dcat-us json file
    dcat_example (HarvestSource):   fixture of a valid dcatus url
    """

    assert isinstance(download_dcatus_catalog(dcat_example.url), dict)


def test_extract_bad_url(dcat_bad_url: HarvestSource):
    """download a bad url.
    dcat_bad_url (HarvestSource):   fixture of a bad url
    """

    res = download_dcatus_catalog(dcat_bad_url.url)

    assert isinstance(res, Exception) and str(res) == "non-200 status code"


def test_extract_bad_json(dcat_bad_json: HarvestSource):
    """download a malformed json.
    dcat_bad_json (HarvestSource)  :   fixture of malformed json
    """

    res = download_dcatus_catalog(dcat_bad_json.url)
    assert isinstance(
        res, Exception
    ) and "Expecting property name enclosed in double quotes" in str(res)


def test_extract_no_dataset_key(dcat_no_dataset_key_json: HarvestSource):
    """download a invalid dcatus catalog.
    dcat_no_dataset_key_json (HarvestSource)
        :   fixture of a dcatus with no "dataset" key
    """
    resp = download_dcatus_catalog(dcat_no_dataset_key_json.url)

    assert "dataset" not in resp
