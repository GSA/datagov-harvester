class CatalogTooDeeplyNested(ValueError):
    """The submitted document exceeds the validator's safe nesting depth."""


NESTING_TOO_DEEP_MESSAGE = (
    "Catalog is nested too deeply to validate. "
    "Flatten the nested catalog or hasPart chains and try again."
)
