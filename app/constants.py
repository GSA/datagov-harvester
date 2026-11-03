# The validator's document limit, advertised on the /validate/ page and checked
# in the browser before it submits to the validator API.
from dcatus_validation.limits import MAX_UPLOAD_BYTES as MAX_UPLOAD_BYTES
from dcatus_validation.limits import MAX_UPLOAD_MB as MAX_UPLOAD_MB
from shared.constants import (
    ORGANIZATION_TYPE_SELECT_CHOICES as ORGANIZATION_TYPE_SELECT_CHOICES,
)
from shared.constants import ORGANIZATION_TYPE_VALUES as ORGANIZATION_TYPE_VALUES

# Largest request body the admin app itself accepts (MAX_CONTENT_LENGTH and
# MAX_FORM_MEMORY_SIZE in create_app). The admin app no longer receives
# catalogs - the /validate/ page submits to the validator app - so this stays
# at its old 10MB rather than following MAX_UPLOAD_MB (GSA/data.gov#6460).
# client_max_body_size in proxy/nginx.conf sits just above it.
ADMIN_MAX_REQUEST_MB = 10
ADMIN_MAX_REQUEST_BYTES = ADMIN_MAX_REQUEST_MB * 1024 * 1024

__all__ = [
    "ORGANIZATION_TYPE_VALUES",
    "ORGANIZATION_TYPE_SELECT_CHOICES",
    "MAX_UPLOAD_MB",
    "MAX_UPLOAD_BYTES",
    "ADMIN_MAX_REQUEST_MB",
    "ADMIN_MAX_REQUEST_BYTES",
]
