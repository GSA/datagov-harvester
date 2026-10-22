# Largest request body the app accepts (MAX_CONTENT_LENGTH and
# MAX_FORM_MEMORY_SIZE in create_app); the same document limit the validator
# API enforces, advertised on the validator page.
from dcatus_validation.limits import MAX_UPLOAD_BYTES as MAX_UPLOAD_BYTES
from dcatus_validation.limits import MAX_UPLOAD_MB as MAX_UPLOAD_MB
from shared.constants import (
    ORGANIZATION_TYPE_SELECT_CHOICES as ORGANIZATION_TYPE_SELECT_CHOICES,
)
from shared.constants import ORGANIZATION_TYPE_VALUES as ORGANIZATION_TYPE_VALUES

__all__ = [
    "ORGANIZATION_TYPE_VALUES",
    "ORGANIZATION_TYPE_SELECT_CHOICES",
    "MAX_UPLOAD_MB",
    "MAX_UPLOAD_BYTES",
]
