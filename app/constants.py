# Largest request body the app accepts (MAX_CONTENT_LENGTH and
# MAX_FORM_MEMORY_SIZE in create_app); the same document limit the validator
# API enforces, advertised on the validator page.
from dcatus_validation.limits import MAX_UPLOAD_BYTES as MAX_UPLOAD_BYTES
from dcatus_validation.limits import MAX_UPLOAD_MB as MAX_UPLOAD_MB
from shared.constants import (
    ORGANIZATION_TYPE_SELECT_CHOICES as ORGANIZATION_TYPE_SELECT_CHOICES,
)
from shared.constants import ORGANIZATION_TYPE_VALUES as ORGANIZATION_TYPE_VALUES

# The one slug rule for organizations and datasets, shared by the admin forms
# and the API schemas so they can't drift apart.
SLUG_REGEX = r"^[a-z0-9-]+$"
SLUG_ERROR_MESSAGE = "Slug can only contain lowercase letters, digits, and hyphens."
ORGANIZATION_SLUG_TAKEN_MESSAGE = "Slug must be unique."
# organization.slug is a String(100) column
ORGANIZATION_SLUG_MAX_LENGTH = 100

__all__ = [
    "SLUG_REGEX",
    "SLUG_ERROR_MESSAGE",
    "ORGANIZATION_SLUG_MAX_LENGTH",
    "ORGANIZATION_SLUG_TAKEN_MESSAGE",
    "ORGANIZATION_TYPE_VALUES",
    "ORGANIZATION_TYPE_SELECT_CHOICES",
    "MAX_UPLOAD_MB",
    "MAX_UPLOAD_BYTES",
]
