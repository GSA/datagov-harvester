from pathlib import Path

from harvester.utils.general_utils import build_dcatus3_validator

ROOT_DIR = Path(__file__).parents[2]
DCATUS3_DEFINITIONS = ROOT_DIR / "schemas" / "dcatus3.0" / "definitions"

DATASET_REF = "https://resources.data.gov/dcat-us/3.0.0/definitions/dataset"

VALID_DATASET = {
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


class TestBuildDcatus3Validator:
    def test_dataset_root_ref_validates_single_dataset(self):
        """With the dataset root ref, a single dataset dict validates standalone."""
        validator = build_dcatus3_validator(DCATUS3_DEFINITIONS, root_ref=DATASET_REF)
        assert validator.is_valid(VALID_DATASET)

    def test_dataset_root_ref_flags_missing_required_field(self):
        """A dataset missing the mandatory contactPoint produces errors."""
        validator = build_dcatus3_validator(DCATUS3_DEFINITIONS, root_ref=DATASET_REF)
        invalid = {k: v for k, v in VALID_DATASET.items() if k != "contactPoint"}
        errors = list(validator.iter_errors(invalid))
        assert errors
        assert any("contactPoint" in e.message for e in errors)

    def test_default_root_ref_validates_catalog(self):
        """The default root ref still validates a whole catalog (web validator tool)."""
        validator = build_dcatus3_validator(DCATUS3_DEFINITIONS)
        catalog = {"@type": "Catalog", "dataset": [VALID_DATASET]}
        assert validator.is_valid(catalog)
