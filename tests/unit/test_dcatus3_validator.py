from pathlib import Path

from harvester.utils.general_utils import (
    build_dcatus3_validator,
    normalize_dataset_identifier,
    open_json,
)

ROOT_DIR = Path(__file__).parents[2]
DCATUS3_DEFINITIONS = ROOT_DIR / "schemas" / "dcatus3.0" / "definitions"
DCATUS3_COMPLETE_EXAMPLE = (
    ROOT_DIR
    / "schemas"
    / "dcatus3.0"
    / "examples"
    / "Dataset"
    / "good"
    / "complete_example.json"
)

DATASET_REF = "https://resources.data.gov/dcat-us/3.0.0/definitions/dataset"


class TestBuildDcatus3Validator:
    def test_dataset_root_ref_validates_single_dataset(self, valid_dcatus3_dataset):
        """With the dataset root ref, a single dataset dict validates standalone."""
        validator = build_dcatus3_validator(DCATUS3_DEFINITIONS, root_ref=DATASET_REF)
        assert validator.is_valid(valid_dcatus3_dataset)

    def test_official_complete_example_dataset_passes_validation(self):
        """Upstream DCAT-US complete dataset example validates against our schema."""
        validator = build_dcatus3_validator(DCATUS3_DEFINITIONS, root_ref=DATASET_REF)
        dataset = open_json(DCATUS3_COMPLETE_EXAMPLE)
        assert validator.is_valid(dataset)

    def test_official_complete_example_identifier_not_harvestable_without_atid(self):
        """Object identifier without @id is not harvestable."""
        dataset = open_json(DCATUS3_COMPLETE_EXAMPLE)
        assert normalize_dataset_identifier(dataset["identifier"]) is None

    def test_dataset_root_ref_flags_missing_required_field(self, valid_dcatus3_dataset):
        """A dataset missing the mandatory contactPoint produces errors."""
        validator = build_dcatus3_validator(DCATUS3_DEFINITIONS, root_ref=DATASET_REF)
        del valid_dcatus3_dataset["contactPoint"]
        errors = list(validator.iter_errors(valid_dcatus3_dataset))
        assert errors
        assert any("contactPoint" in e.message for e in errors)

    def test_default_root_ref_validates_catalog(self, valid_dcatus3_dataset):
        """The default root ref still validates a whole catalog (web validator tool)."""
        validator = build_dcatus3_validator(DCATUS3_DEFINITIONS)
        catalog = {"@type": "Catalog", "dataset": [valid_dcatus3_dataset]}
        assert validator.is_valid(catalog)
