import uuid

from flask_sqlalchemy import SQLAlchemy
from geoalchemy2 import Geometry
from sqlalchemy import Column, Enum, String, func
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    __abstract__ = True  # Indicates that this class should not be created as a table
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))


# For ref: https://stackoverflow.com/questions/22698478/what-is-the-difference-between-the-declarative-base-and-db-model
db = SQLAlchemy(model_class=Base)


class Error(db.Model):
    __abstract__ = True
    date_created = db.Column(db.DateTime, default=func.statement_timestamp())
    type = db.Column(db.String)
    message = db.Column(db.String)


class Organization(db.Model):
    __tablename__ = "organization"

    name = db.Column(db.String, nullable=False, index=True)
    logo = db.Column(db.String)
    sources = db.relationship(
        "HarvestSource", backref="org", cascade="all, delete-orphan", lazy=True
    )


class HarvestSource(db.Model):
    __tablename__ = "harvest_source"

    organization_id = db.Column(
        db.String(36), db.ForeignKey("organization.id"), nullable=False
    )
    name = db.Column(db.String, nullable=False)
    url = db.Column(db.String, nullable=False, unique=True)
    notification_emails = db.Column(db.ARRAY(db.String))
    frequency = db.Column(
        Enum(
            "manual",
            "daily",
            "weekly",
            "biweekly",
            "monthly",
            name="frequency",
        ),
        nullable=False,
        index=True,
    )
    schema_type = db.Column(
        db.Enum(
            "iso19115_1",
            "iso19115_2",
            "csdgm",
            "dcatus1.1: federal",
            "dcatus1.1: non-federal",
            name="schema_type",
        ),
        nullable=False,
    )

    source_type = db.Column(
        db.Enum("document", "waf", name="source_type"), nullable=False
    )
    jobs = db.relationship(
        "HarvestJob", backref="source", cascade="all, delete-orphan", lazy=True
    )
    notification_frequency = db.Column(
        db.Enum(
            "on_error",
            "always",
            name="notification_frequency",
        ),
        nullable=False,
    )


class HarvestJob(db.Model):
    __tablename__ = "harvest_job"

    harvest_source_id = db.Column(
        db.String(36), db.ForeignKey("harvest_source.id"), nullable=False
    )
    status = db.Column(
        Enum(
            "in_progress",
            "complete",
            "new",
            "error",
            name="job_status",
        ),
        nullable=False,
        index=True,
    )
    job_type = db.Column(db.String(20), default="harvest")
    date_created = db.Column(
        db.DateTime, index=True, default=func.statement_timestamp()
    )
    date_finished = db.Column(db.DateTime)
    records_total = db.Column(db.Integer, default=0)
    records_added = db.Column(db.Integer, default=0)
    records_updated = db.Column(db.Integer, default=0)
    records_deleted = db.Column(db.Integer, default=0)
    records_errored = db.Column(db.Integer, default=0)
    records_ignored = db.Column(db.Integer, default=0)
    records_validated = db.Column(db.Integer, default=0)
    errors = db.relationship(
        "HarvestJobError", backref="job", cascade="all, delete-orphan", lazy=True
    )
    records = db.relationship(
        "HarvestRecord", backref="job", cascade="all, delete-orphan", lazy=True
    )
    record_errors = db.relationship(
        "HarvestRecordError", backref="job", cascade="all, delete-orphan", lazy=True
    )


class HarvestRecord(db.Model):
    __tablename__ = "harvest_record"

    identifier = db.Column(db.String, nullable=False)
    harvest_job_id = db.Column(
        db.String(36), db.ForeignKey("harvest_job.id"), nullable=False
    )
    harvest_source_id = db.Column(
        db.String(36), db.ForeignKey("harvest_source.id"), nullable=False
    )
    source_hash = db.Column(db.String)
    source_raw = db.Column(db.String)
    date_created = db.Column(
        db.DateTime, index=True, default=func.statement_timestamp()
    )
    date_finished = db.Column(db.DateTime, index=True)
    ckan_id = db.Column(db.String, index=True)
    ckan_name = db.Column(db.String, index=True)
    action = db.Column(
        Enum("create", "update", "delete", name="record_action"), index=True
    )
    status = db.Column(Enum("error", "success", name="record_status"), index=True)
    errors = db.relationship("HarvestRecordError", backref="record", lazy=True)


class HarvestJobError(Error):
    __tablename__ = "harvest_job_error"

    harvest_job_id = db.Column(
        db.String(36), db.ForeignKey("harvest_job.id"), nullable=False
    )


class HarvestRecordError(Error):
    __tablename__ = "harvest_record_error"

    harvest_record_id = db.Column(
        db.String, db.ForeignKey("harvest_record.id"), nullable=True
    )
    harvest_job_id = db.Column(
        db.String(36), db.ForeignKey("harvest_job.id"), nullable=False
    )


class HarvestUser(db.Model):
    __tablename__ = "harvest_user"
    email = db.Column(db.String(120), unique=True, nullable=False)
    name = db.Column(db.String(120), nullable=True)
    ssoid = db.Column(db.String(200), unique=True, nullable=True)


class Locations(db.Model):
    __tablename__ = "locations"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String)
    type = db.Column(db.String)
    display_name = db.Column(db.String)
    the_geom = db.Column(Geometry(geometry_type="MULTIPOLYGON"))
    type_order = db.Column(db.Integer)
