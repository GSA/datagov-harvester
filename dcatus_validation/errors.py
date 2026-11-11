class CatalogTooDeeplyNested(ValueError):
    """The submitted document exceeds the validator's safe nesting depth."""


VALIDATION_TIMED_OUT_MESSAGE = (
    "This catalog took too long to check. "
    "Try validating a smaller catalog, for example by splitting it into parts."
)

NESTING_TOO_DEEP_MESSAGE = (
    "Catalog is nested too deeply to validate. "
    "Flatten the nested catalog or hasPart chains and try again."
)
