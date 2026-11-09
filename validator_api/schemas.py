import marshmallow
from apiflask import Schema, validators
from apiflask.fields import URL, Boolean, Integer, List, Nested, Raw, String
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


class ValidationWarningSchema(Schema):
    identifier = Raw(
        required=True,
        metadata={
            "description": (
                "The record's identifier, or its position among records of its "
                "type (an integer) when it has none."
            )
        },
    )
    record_type = String(
        required=True,
        metadata={
            "description": (
                "dataset, data_service, catalog_record, or data_series. DCAT-US "
                "1.1 catalogs only have datasets."
            )
        },
    )
    warning_type = String(
        required=True,
        metadata={
            "description": (
                "Stable slug for the kind of warning, the same value the "
                "harvester records for it."
            )
        },
    )
    message = String(required=True)
    title = String(
        required=True,
        allow_none=True,
        metadata={"description": "The record's title, or null when it has none."},
    )


class IssueSummaryRowSchema(Schema):
    severity = String(
        required=True,
        validate=validate.OneOf(["error", "warning"]),
    )
    type = String(
        required=True,
        metadata={
            "description": (
                "The issue type a harvest records: ValidationException for schema "
                "errors, DuplicateIdentifierException for duplicate identifiers, "
                "or a warning's warning_type."
            )
        },
    )
    count = Integer(required=True)


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
    validation_warnings = List(
        Nested(ValidationWarningSchema),
        required=True,
        metadata={
            "description": (
                "Content-quality issues a harvest would report. Warnings never "
                "make a catalog invalid."
            )
        },
    )
    issue_summary = List(
        Nested(IssueSummaryRowSchema),
        required=True,
        metadata={
            "description": (
                "Count of returned errors and warnings by severity and type, "
                "ordered by severity then type, like a harvest job's record issue "
                "summary."
            )
        },
    )
    warnings_incomplete = Boolean(
        metadata={
            "description": (
                "True when warning checks stopped at the warning limit, or could "
                "not run because the catalog couldn't be split into records."
            )
        }
    )


class ValidationErrorResponseSchema(Schema):
    error = String(required=True)
