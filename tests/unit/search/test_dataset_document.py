from copy import deepcopy

from search.documents import DatasetDocument


def _set_dcat(dataset, **updates):
    dcat = deepcopy(dataset.dcat or {})
    dcat.update(updates)
    dataset.dcat = dcat
    return dataset


def test_dataset_to_document_sets_access_level(mock_dataset_with_datetime):
    mock_dataset_with_datetime.dcat = {
        "accessRights": "Not available for public release. "
        "Contact the publisher for more information."
    }

    document = DatasetDocument(mock_dataset_with_datetime).dataset_to_document()

    assert document["access_level"] == "non-public"


def test_dataset_to_document_normalizes_access_level(mock_dataset_with_datetime):
    _set_dcat(mock_dataset_with_datetime, accessLevel=" Restricted Public ")

    document = DatasetDocument(mock_dataset_with_datetime).dataset_to_document()

    assert document["access_level"] == "restricted public"


def test_dataset_to_document_falls_back_to_access_rights(mock_dataset_with_datetime):
    mock_dataset_with_datetime.dcat = {
        "accessRights": "Not available for public release. "
        "Contact the publisher for more information."
    }

    document = DatasetDocument(mock_dataset_with_datetime).dataset_to_document()

    assert document["access_level"] == "non-public"


def test_dataset_to_document_falls_back_to_access_rights_restricted(
    mock_dataset_with_datetime,
):
    mock_dataset_with_datetime.dcat = {
        "accessRights": "Access restricted. Contact the publisher to request access."
    }
    document = DatasetDocument(mock_dataset_with_datetime).dataset_to_document()
    assert document["access_level"] == "restricted public"


def test_dataset_to_document_falls_back_to_access_rights_allows_agency_specific_wording(
    mock_dataset_with_datetime,
):
    # Guards against relying on the migration guide's exact (non-authoritative)
    # sample sentences. Agencies following the implementation guide's
    # flexibility language may phrase this differently.
    mock_dataset_with_datetime.dcat = {
        "accessRights": "This dataset is restricted to approved researchers "
        "who have signed a data use agreement."
    }
    document = DatasetDocument(mock_dataset_with_datetime).dataset_to_document()
    assert document["access_level"] == "restricted public"


def test_dataset_to_document_prefers_access_level_over_access_rights(
    mock_dataset_with_datetime,
):
    mock_dataset_with_datetime.dcat = {
        "accessLevel": "restricted public",
        "accessRights": "public",
    }

    document = DatasetDocument(mock_dataset_with_datetime).dataset_to_document()

    assert document["access_level"] == "restricted public"
