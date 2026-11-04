import marshmallow
from apiflask import Schema, validators
from apiflask.fields import URL, Boolean, List, Raw, String
from marshmallow import ValidationError, validate

from dcatus_validation.fetch import ALLOW_PRIVATE_ADDRESSES
from dcatus_validation.validate import SCHEMA_NAMES


class ValidatorInfo(Schema):
    schema = String(
        required=True,
        validate=validators.OneOf(SCHEMA_NAMES),
    )
    fetch_method = String(
        required=True,
        validate=validators.OneOf(
            [
                "url",
                "paste",
            ]
        ),
    )
    # Hostnames without a TLD (the compose network's `nginx-harvest-source`)
    # only make sense where private addresses may be fetched at all.
    url = URL(require_tld=not ALLOW_PRIVATE_ADDRESSES)

    @marshmallow.validates_schema
    def validate_url(self, data, **kwargs):
        if data.get("fetch_method") == "url" and not data.get("url"):
            raise ValidationError("'url' field is required when fetch_method is 'url'")

    # Parsed in the route rather than here, so a parse failure is reported with
    # its position (see dcatus_validation.fetch.invalid_json_message) and the
    # document is only parsed once.
    json_text = String()

    @marshmallow.validates_schema
    def validate_json_text(self, data, **kwargs):
        if data.get("fetch_method") == "paste" and not data.get("json_text"):
            raise ValidationError(
                "'json_text' field is required when fetch_method is 'paste'"
            )


class DocumentQuery(Schema):
    """Query parameters of /validate/document, whose body is the catalog."""

    schema = String(
        required=True,
        validate=validators.OneOf(SCHEMA_NAMES),
    )
    fetch_method = String(
        load_default="upload",
        validate=validators.OneOf(["paste", "upload"]),
        metadata={
            "description": (
                "`paste`: the body is pasted text, as UTF-8. `upload` (the "
                "default): the body is a file, UTF-8 or UTF-16."
            )
        },
    )


class ValidationResultSchema(Schema):
    validation_errors = List(
        # [identifier, message]. The identifier is the dataset's `identifier`,
        # its position in `dataset` when it has none (an integer), or "" for
        # DCAT-US 3.0, whose messages carry the JSON path instead.
        List(
            Raw(),
            validate=validate.Length(equal=2),
        ),
        required=True,
    )
    validation_incomplete = Boolean(
        metadata={
            "description": (
                "True when the public service stopped after reaching its error "
                "limit. Fix the returned errors and validate again."
            )
        }
    )


class ValidationErrorResponseSchema(Schema):
    error = String(required=True)
