import uuid
from typing import Optional

from flask_sqlalchemy import SQLAlchemy
from geoalchemy2 import Geometry
from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import DeclarativeBase, backref, column_property, relationship

from shared.constants import (
    FREQUENCY_VALUES,
    JOB_STATUS_VALUES,
    NOTIFICATION_FREQUENCY_VALUES,
    ORGANIZATION_TYPE_VALUES,
    RECORD_STATUS_VALUES,
    RECORD_TYPE_VALUES,
    SCHEMA_TYPE_VALUES,
    SEVERITY_VALUES,
    SOURCE_TYPE_VALUES,
)


class Base(DeclarativeBase):
    __abstract__ = True  # Indicates that this class should not be created as a table
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Error(Base):
    __abstract__ = True
    date_created = Column(DateTime, default=func.statement_timestamp())
    type = Column(String)
    message = Column(String)


class Organization(Base):
    __tablename__ = "organization"
    __table_args__ = (UniqueConstraint("slug", name="uq_organization_slug"),)

    name = Column(String, nullable=False, index=True)
    logo = Column(String)
    description = Column(Text)
    slug = Column(String(100), nullable=False)

    organization_type = Column(
        Enum(
            *ORGANIZATION_TYPE_VALUES,
            name="organization_type_enum",
            create_constraint=True,
        )
    )

    aliases = Column(ARRAY(String))

    sources = relationship(
        "HarvestSource",
        backref=backref("org", lazy="joined"),
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy=True,
    )


class HarvestSource(Base):
    __tablename__ = "harvest_source"

    __table_args__ = (
        CheckConstraint(
            "(collection_parent_url IS NULL"
            " AND source_type <> 'waf-collection')"
            " OR (collection_parent_url IS NOT NULL"
            " AND source_type = 'waf-collection')",
            name="wafcollectionparenturl",
        ),
        Index("ix_harvest_source_organization_id", "organization_id"),
    )

    organization_id = Column(
        String(36),
        ForeignKey("organization.id", ondelete="CASCADE"),
        nullable=False,
    )

    name = Column(String, nullable=False)
    url = Column(String, nullable=False, unique=True)
    notification_emails = Column(ARRAY(String))

    frequency = Column(
        Enum(
            *FREQUENCY_VALUES,
            name="frequency",
        ),
        nullable=False,
        index=True,
    )

    schema_type = Column(
        Enum(
            *SCHEMA_TYPE_VALUES,
            name="schema_type",
        ),
        nullable=False,
    )

    source_type = Column(
        Enum(
            *SOURCE_TYPE_VALUES,
            name="source_type",
        ),
        nullable=False,
    )

    jobs = relationship(
        "HarvestJob",
        backref=backref("source", lazy="joined"),
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy=True,
    )

    notification_frequency = Column(
        Enum(
            *NOTIFICATION_FREQUENCY_VALUES,
            name="notification_frequency",
        ),
        nullable=False,
    )

    collection_parent_url = Column(String)


# to avoid moving models around adding this here since it references
# 2 models (Organization & HarvestSource)
Organization.source_count = column_property(
    select(func.count(HarvestSource.id))
    .where(HarvestSource.organization_id == Organization.id)
    .correlate_except(HarvestSource)
    .scalar_subquery()
)


class HarvestJob(Base):
    __tablename__ = "harvest_job"

    __table_args__ = (Index("ix_harvest_job_harvest_source_id", "harvest_source_id"),)

    harvest_source_id = Column(
        String(36),
        ForeignKey("harvest_source.id", ondelete="CASCADE"),
        nullable=False,
    )

    status = Column(
        Enum(
            *JOB_STATUS_VALUES,
            name="job_status",
        ),
        nullable=False,
        index=True,
    )

    job_type = Column(String(20), default="harvest")
    date_created = Column(DateTime, index=True, default=func.statement_timestamp())
    date_finished = Column(DateTime)

    records_total = Column(Integer, default=0)
    records_added = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_deleted = Column(Integer, default=0)
    records_errored = Column(Integer, default=0)
    records_warned = Column(Integer, default=0)
    records_ignored = Column(Integer, default=0)
    records_validated = Column(Integer, default=0)

    # Catalog-level DCAT-US 3.0 metadata for this job's source, with dataset,
    # service, record, and catalog fields stripped (those are harvested
    # separately as their own records). Null for non-Catalog sources.
    dcatus_catalog = Column(JSONB)

    errors = relationship(
        "HarvestJobError",
        backref=backref("job", lazy="joined"),
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy=True,
    )

    records = relationship(
        "HarvestRecord",
        backref="job",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy=True,
    )

    record_errors = relationship(
        "HarvestRecordError",
        backref="job",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy=True,
    )


class HarvestRecord(Base):
    __tablename__ = "harvest_record"

    identifier = Column(String, nullable=False)

    harvest_job_id = Column(
        String(36),
        ForeignKey("harvest_job.id", ondelete="CASCADE"),
        nullable=False,
    )

    harvest_source_id = Column(
        String(36),
        ForeignKey("harvest_source.id", ondelete="CASCADE"),
        nullable=False,
    )

    source_hash = Column(String)
    source_raw = Column(String)
    source_transform = Column(JSONB)

    date_created = Column(DateTime, index=True, default=func.statement_timestamp())
    date_finished = Column(DateTime, index=True)

    ckan_id = Column(String, index=True)

    action = Column(
        Enum("create", "update", "delete", name="record_action"),
        index=True,
    )

    # Parent information is not in source_raw for XML records.
    parent_identifier = Column(String)

    status = Column(
        Enum(*RECORD_STATUS_VALUES, name="record_status"),
        index=True,
    )

    # Defaults to "dataset": every record was a dataset before DCAT-US 3.0.
    record_type = Column(
        Enum(*RECORD_TYPE_VALUES, name="record_type"),
        nullable=False,
        server_default="dataset",
        index=True,
    )

    # No delete cascade here on purpose: record errors outlive their record so
    # error history survives record cleanup (see test_harvest_record_error_remains).
    # harvest_record_id is nullable and stays SET NULL; the errors are still
    # cleaned up when the owning job or source goes away, via harvest_job_id.
    errors = relationship("HarvestRecordError", backref="record", lazy=True)

    __table_args__ = (
        Index("ix_harvest_record_harvest_job_id", "harvest_job_id"),
        Index(
            "ix_harvest_record_source_type_identifier_created_success",
            harvest_source_id,
            record_type,
            identifier,
            date_created.desc(),
            postgresql_where=text("status = 'success'"),
            postgresql_include=["action"],
        ),
    )

    @property
    def dataset_slug(self) -> Optional[str]:
        dataset = getattr(self, "dataset", None)
        if dataset is None:
            return None
        return dataset.slug


class Dataset(Base):
    __tablename__ = "dataset"

    # Base has a string `id` column that is UUID by default.
    # slug is the string used in a URL for this dataset.
    slug = Column(String, nullable=False, index=True, unique=True)

    # Full dataset details in DCAT schema.
    # MutableDict tracks in-place mutations, for example:
    # dcat["spatial"] = "..."
    dcat = Column(MutableDict.as_mutable(JSONB), nullable=False)

    translated_spatial = Column(JSONB)

    organization_id = Column(
        String(36),
        ForeignKey("organization.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    harvest_source_id = Column(
        String(36),
        ForeignKey("harvest_source.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    harvest_record_id = Column(
        String(36),
        ForeignKey("harvest_record.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        unique=True,
    )

    popularity = Column(Integer, server_default="0")
    last_harvested_date = Column(DateTime, index=True)

    # The `datasets` / `dataset` backrefs below are the one-to-many side, so
    # passive_deletes belongs on the backref. Without it SQLAlchemy tries to NULL
    # these non-nullable FKs when a parent is deleted, raising IntegrityError.
    # "all" rather than True so a warm collection is handled by the database too,
    # and without delete-orphan, which would delete a row merely reassigned to a
    # different parent.
    organization = relationship(
        "Organization",
        backref=backref("datasets", lazy=True, passive_deletes="all"),
        lazy="joined",
    )

    harvest_source = relationship(
        "HarvestSource",
        backref=backref("datasets", lazy=True, passive_deletes="all"),
        lazy="joined",
    )

    harvest_record = relationship(
        "HarvestRecord",
        backref=backref("dataset", uselist=False, lazy=True, passive_deletes="all"),
        lazy="joined",
        uselist=False,
    )


class DatasetViewCount(Base):
    __tablename__ = "dataset_view_count"

    dataset_slug = Column(String(100), nullable=False, unique=True, index=True)
    view_count = Column(Integer, nullable=False, default=0)


class ResourceViewCount(Base):
    __tablename__ = "resource_view_count"

    # URL from Google Analytics resource path is truncated to 100 characters.
    resource_url = Column(String(100), nullable=False, unique=True, index=True)
    view_count = Column(Integer, nullable=False, default=0)


class HarvestJobError(Error):
    __tablename__ = "harvest_job_error"

    harvest_job_id = Column(
        String(36),
        ForeignKey("harvest_job.id", ondelete="CASCADE"),
        nullable=False,
    )

    __table_args__ = (Index("ix_harvest_job_error_harvest_job_id", "harvest_job_id"),)


class HarvestRecordError(Error):
    __tablename__ = "harvest_record_error"

    # SET NULL, not CASCADE: a record error outlives the record it describes.
    # Deleting a job or source still removes it via harvest_job_id's CASCADE.
    harvest_record_id = Column(
        String,
        ForeignKey("harvest_record.id", ondelete="SET NULL"),
        nullable=True,
    )

    harvest_job_id = Column(
        String(36),
        ForeignKey("harvest_job.id", ondelete="CASCADE"),
        nullable=False,
    )

    severity = Column(
        Enum(*SEVERITY_VALUES, name="error_severity"),
        nullable=False,
        server_default="error",
    )

    __table_args__ = (
        Index("ix_hre_job_id", "harvest_job_id"),
        Index("ix_harvest_record_error_harvest_record_id", "harvest_record_id"),
    )


class HarvestUser(Base):
    __tablename__ = "harvest_user"

    email = Column(String(120), unique=True, nullable=False)
    name = Column(String(120), nullable=True)
    ssoid = Column(String(200), unique=True, nullable=True)


class Locations(Base):
    __tablename__ = "locations"

    name = Column(String)
    type = Column(String)
    display_name = Column(String)
    the_geom = Column(Geometry(geometry_type="MULTIPOLYGON"))
    type_order = Column(Integer)


db = SQLAlchemy(model_class=Base)
