import pytest

from harvester.utils.general_utils import (
    build_dcatus3_validator,
    normalize_dataset_identifier,
    open_json,
)
from harvester.utils.schema_paths import (
    DCATUS3_COMPLETE_EXAMPLE,
    DCATUS3_DEFINITIONS_DIR,
)

DATASET_REF = "https://resources.data.gov/dcat-us/3.0.0/definitions/dataset"
DATASET_VALIDATOR = build_dcatus3_validator(
    DCATUS3_DEFINITIONS_DIR, root_ref=DATASET_REF
)

DATASERVICE_REF = "https://resources.data.gov/dcat-us/3.0.0/definitions/dataservice"
DATASERVICE_VALIDATOR = build_dcatus3_validator(
    DCATUS3_DEFINITIONS_DIR, root_ref=DATASERVICE_REF
)

CATALOGRECORD_REF = "https://resources.data.gov/dcat-us/3.0.0/definitions/catalogrecord"
CATALOGRECORD_VALIDATOR = build_dcatus3_validator(
    DCATUS3_DEFINITIONS_DIR, root_ref=CATALOGRECORD_REF
)


@pytest.fixture
def valid_dcatus3_dataset() -> dict:
    return {
        "@type": "Dataset",
        "title": "Test Dataset",
        "description": "A valid DCAT-US 3.0 dataset.",
        "identifier": "https://example.gov/datasets/one",
        "publisher": {"@type": "Organization", "name": "Test Agency"},
        "contactPoint": {
            "@type": "Kind",
            "fn": "Test Contact",
            "hasEmail": "mailto:test@example.gov",
        },
    }


@pytest.fixture
def valid_dcatus3_dataservice() -> dict:
    return {
        "@type": "DataService",
        "title": "Test Data Service",
        "description": "A valid DCAT-US 3.0 data service.",
        "identifier": "https://example.gov/services/one",
        "endpointURL": ["https://api.example.gov/v1"],
        "publisher": {"@type": "Organization", "name": "Test Agency"},
        "contactPoint": [
            {
                "@type": "Kind",
                "fn": "Test Contact",
                "hasEmail": "mailto:test@example.gov",
            }
        ],
    }


@pytest.fixture
def valid_dcatus3_catalogrecord() -> dict:
    return {
        "@type": "CatalogRecord",
        "title": "Test Catalog Record",
        "modified": "2024-06-15",
        "primaryTopic": "https://example.gov/datasets/one",
    }


class TestBuildDcatus3Validator:
    def test_dataset_root_ref_validates_single_dataset(self, valid_dcatus3_dataset):
        """With the dataset root ref, a single dataset dict validates standalone."""
        assert DATASET_VALIDATOR.is_valid(valid_dcatus3_dataset)

    def test_official_complete_example_dataset_passes_validation(self):
        """Upstream DCAT-US complete dataset example validates against our schema."""
        dataset = open_json(DCATUS3_COMPLETE_EXAMPLE)
        assert DATASET_VALIDATOR.is_valid(dataset)

    def test_official_complete_example_identifier_not_harvestable_without_atid(self):
        """Object identifier without @id is not harvestable."""
        dataset = open_json(DCATUS3_COMPLETE_EXAMPLE)
        assert normalize_dataset_identifier(dataset["identifier"]) is None

    def test_dataset_root_ref_flags_missing_required_field(self, valid_dcatus3_dataset):
        """A dataset missing the mandatory contactPoint produces errors."""
        del valid_dcatus3_dataset["contactPoint"]
        errors = list(DATASET_VALIDATOR.iter_errors(valid_dcatus3_dataset))
        assert errors
        assert any("contactPoint" in e.message for e in errors)

    def test_default_root_ref_validates_catalog(self, valid_dcatus3_dataset):
        """The default root ref still validates a whole catalog (web validator tool)."""
        validator = build_dcatus3_validator(DCATUS3_DEFINITIONS_DIR)
        catalog = {"@type": "Catalog", "dataset": [valid_dcatus3_dataset]}
        assert validator.is_valid(catalog)


class TestBuildDcatus3ValidatorDataService:
    def test_dataservice_root_ref_validates_single_dataservice(
        self, valid_dcatus3_dataservice
    ):
        """With the dataservice root ref, a single DataService dict validates
        standalone."""
        assert DATASERVICE_VALIDATOR.is_valid(valid_dcatus3_dataservice)

    def test_dataservice_root_ref_flags_missing_required_field(
        self, valid_dcatus3_dataservice
    ):
        """A DataService missing the mandatory endpointURL produces errors."""
        del valid_dcatus3_dataservice["endpointURL"]
        errors = list(DATASERVICE_VALIDATOR.iter_errors(valid_dcatus3_dataservice))
        assert errors
        assert any("endpointURL" in e.message for e in errors)

    def test_dataservice_root_ref_does_not_require_identifier(
        self, valid_dcatus3_dataservice
    ):
        """DCAT-US 3.0 doesn't mark identifier as schema-required for DataService;
        the harvester enforces it separately (see filter_datasets_with_no_identifier).
        """
        del valid_dcatus3_dataservice["identifier"]
        assert DATASERVICE_VALIDATOR.is_valid(valid_dcatus3_dataservice)


class TestBuildDcatus3ValidatorCatalogRecord:
    def test_catalogrecord_root_ref_validates_single_catalogrecord(
        self, valid_dcatus3_catalogrecord
    ):
        """With the catalogrecord root ref, a single CatalogRecord dict
        validates standalone."""
        assert CATALOGRECORD_VALIDATOR.is_valid(valid_dcatus3_catalogrecord)

    def test_catalogrecord_root_ref_flags_missing_required_field(
        self, valid_dcatus3_catalogrecord
    ):
        """A CatalogRecord missing the mandatory primaryTopic produces errors."""
        del valid_dcatus3_catalogrecord["primaryTopic"]
        errors = list(CATALOGRECORD_VALIDATOR.iter_errors(valid_dcatus3_catalogrecord))
        assert errors
        assert any("primaryTopic" in e.message for e in errors)

    def test_catalogrecord_root_ref_does_not_require_id(
        self, valid_dcatus3_catalogrecord
    ):
        """CatalogRecord has no "identifier" field at all, only an optional
        "@id"; the harvester enforces @id separately (see
        filter_datasets_with_no_identifier)."""
        assert CATALOGRECORD_VALIDATOR.is_valid(valid_dcatus3_catalogrecord)
        assert "@id" not in valid_dcatus3_catalogrecord
