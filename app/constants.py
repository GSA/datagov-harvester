from shared.constants import (
    ORGANIZATION_TYPE_SELECT_CHOICES as ORGANIZATION_TYPE_SELECT_CHOICES,
)
from shared.constants import ORGANIZATION_TYPE_VALUES as ORGANIZATION_TYPE_VALUES

# Largest request body the app accepts. Applied to both MAX_CONTENT_LENGTH (the
# whole request) and MAX_FORM_MEMORY_SIZE (non-file form fields) in create_app,
# and advertised to the user on the validator page.
MAX_UPLOAD_MB = 10
MAX_UPLOAD_BYTES = MAX_UPLOAD_MB * 1024 * 1024

__all__ = [
    "ORGANIZATION_TYPE_VALUES",
    "ORGANIZATION_TYPE_SELECT_CHOICES",
    "MAX_UPLOAD_MB",
    "MAX_UPLOAD_BYTES",
]
