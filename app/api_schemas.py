from enum import Enum as PyEnum
import os
import json

from apiflask import Schema, validators
from apiflask.fields import (
    UUID,
    Boolean,
    DateTime,
    Dict,
    Enum,
    Integer,
    List,
    String,
    URL,
)
import marshmallow
from marshmallow import ValidationError, validate

from shared.constants import (
    ACTION_VALUES,
    FREQUENCY_VALUES,
    JOB_STATUS_VALUES,
    NOTIFICATION_FREQUENCY_VALUES,
    ORGANIZATION_TYPE_VALUES,
    RECORD_STATUS_VALUES,
    SCHEMA_TYPE_VALUES,
    SOURCE_TYPE_VALUES,
)

IS_PROD = os.getenv("FLASK_ENV") == "production"


def _to_enum(name, values):
    """Make an iterable of values into a Python enum.

    The produced enum has the name and values the same.
    """
    return PyEnum(name, [(a, a) for a in values])


ACTION_ENUM = _to_enum("Action", ACTION_VALUES)
FREQUENCY_ENUM = _to_enum("Frequency", FREQUENCY_VALUES)
JOB_STATUS_ENUM = _to_enum("JobStatus", JOB_STATUS_VALUES)
NOTIFICATION_FREQUENCY_ENUM = _to_enum(
    "NotificationFrequency", NOTIFICATION_FREQUENCY_VALUES
)
ORGANIZATION_TYPE_ENUM = _to_enum("OrganizationType", ORGANIZATION_TYPE_VALUES)
RECORD_STATUS_ENUM = _to_enum("RecordStatus", RECORD_STATUS_VALUES)
SCHEMA_TYPE_ENUM = _to_enum("SchemaType", SCHEMA_TYPE_VALUES)
SOURCE_TYPE_ENUM = _to_enum("SourceType", SOURCE_TYPE_VALUES)


# must be created from a dict because "type" key
ErrorInfo = Schema.from_dict(
    {
        "id": UUID(required=True),
        "date_created": DateTime(),
        "type": String(),
        "message": String(),
        "harvest_record_id": UUID(required=True),
        "harvest_job_id": UUID(required=True),
    },
    name="ErrorInfo",
)


class JobInfo(Schema):
    id = UUID(required=True)
    harvest_source_id = UUID(required=True)
    status = Enum(JOB_STATUS_ENUM, required=True)
    job_type = String()
    date_created = DateTime()
    date_finished = DateTime()
    records_total = Integer()
    records_added = Integer()
    records_updated = Integer()
    records_deleted = Integer()
    records_errored = Integer()
    records_ignored = Integer()
    records_validated = Integer()


class OrgCreate(Schema):
    name = String(required=True)
    logo = String()
    description = String()
    slug = String(validate=validators.Length(max=100))
    organization_type = Enum(ORGANIZATION_TYPE_ENUM)
    aliases = List(String())


class OrgInfo(OrgCreate):
    id = UUID(required=True)


class QueryInfo(Schema):
    """Query input for various object types."""

    harvest_job_id = UUID()
    harvest_source_id = UUID()
    facets = String()
    page = Integer()
    per_page = Integer()
    count = Boolean()
    order_by = String()


class RecordInfo(Schema):
    id = UUID(required=True)
    identifier = String(required=True)
    harvest_job_id = UUID(required=True)
    harvest_source_id = UUID(required=True)
    source_hash = String()
    source_raw = String()
    source_transform = Dict()
    date_created = DateTime()
    date_finished = DateTime()
    ckan_id = String()
    action = Enum(ACTION_ENUM)
    parent_identifier = String()
    status = Enum(RECORD_STATUS_ENUM)


class SourceInfo(Schema):
    id = String(required=True)
    organization_id = UUID(required=True)
    name = String(required=True)
    url = String(required=True)
    notification_emails = List(String())
    frequency = Enum(FREQUENCY_ENUM, required=True)
    schema_type = Enum(SCHEMA_TYPE_ENUM, required=True)
    source_type = Enum(SOURCE_TYPE_ENUM, required=True)
    notification_frequency = Enum(NOTIFICATION_FREQUENCY_ENUM, required=True)
    collection_parent_url = String()


class ValidatorInfo(Schema):
    schema = String(
        required=True,
        validate=validators.OneOf(
            [
                "dcatus1.1: federal dataset",
                "dcatus1.1: non-federal dataset",
            ]
        ),
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
    url = URL(require_tld=IS_PROD)

    @marshmallow.validates_schema
    def validate_url(self, data, **kwargs):
        if data.get("fetch_method") == "url" and not data.get("url"):
            raise ValidationError("'url' field is required when fetch_method is 'url'")

    json_text = String()

    @marshmallow.validates_schema
    def validate_json_text(self, data, **kwargs):
        if data.get("fetch_method") == "paste":
            if not data.get("json_text"):
                raise ValidationError(
                    "'json_text' field is required when fetch_method is 'paste'"
                )
            else:
                try:
                    json.loads(data.get("json_text"))
                except json.JSONDecodeError:
                    raise ValidationError("Invalid JSON")


class ValidationResultSchema(Schema):
    validation_errors = List(
        List(
            String(),
            validate=validate.Length(equal=2),
        ),
        required=True,
    )


class ValidationErrorResponseSchema(Schema):
    error = String(required=True)
