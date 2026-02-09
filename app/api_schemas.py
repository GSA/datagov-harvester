from enum import Enum as PyEnum

from apiflask import Schema, validators
from apiflask.fields import UUID, Boolean, DateTime, Dict, Enum, Integer, List, String

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

ACTION_ENUM = PyEnum("Action", [(a, a) for a in ACTION_VALUES])
FREQUENCY_ENUM = PyEnum("Frequency", [(a, a) for a in FREQUENCY_VALUES])
JOB_STATUS_ENUM = PyEnum("JobStatus", [(a, a) for a in JOB_STATUS_VALUES])
NOTIFICATION_FREQUENCY_ENUM = PyEnum(
    "NotificationFrequency", [(a, a) for a in NOTIFICATION_FREQUENCY_VALUES]
)
ORGANIZATION_TYPE_ENUM = PyEnum(
    "OrganizationType", [(a, a) for a in ORGANIZATION_TYPE_VALUES]
)
RECORD_STATUS_ENUM = PyEnum("RecordStatus", [(a, a) for a in RECORD_STATUS_VALUES])
SCHEMA_TYPE_ENUM = PyEnum("SchemaType", [(a, a) for a in SCHEMA_TYPE_VALUES])
SOURCE_TYPE_ENUM = PyEnum("SourceType", [(a, a) for a in SOURCE_TYPE_VALUES])


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
