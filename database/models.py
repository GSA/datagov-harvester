import uuid

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Enum, String, func
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    __abstract__ = True  # Indicates that this class should not be created as a table
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))


# For ref: https://stackoverflow.com/questions/22698478/what-is-the-difference-between-the-declarative-base-and-db-model
db = SQLAlchemy(model_class=Base)


class Error(db.Model):
    __abstract__ = True
    date_created = db.Column(db.DateTime, default=func.now())
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

    name = db.Column(db.String, nullable=False)
    notification_emails = db.Column(db.ARRAY(db.String))
    organization_id = db.Column(
        db.String(36), db.ForeignKey("organization.id"), nullable=False
    )
    frequency = db.Column(db.String, nullable=False)
    user_requested_frequency = db.Column(db.String)
    url = db.Column(db.String, nullable=False, unique=True)
    schema_type = db.Column(db.String, nullable=False)
    source_type = db.Column(db.String, nullable=False)
    status = db.Column(db.String)
    jobs = db.relationship(
        "HarvestJob", backref="source", cascade="all, delete-orphan", lazy=True
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
            "manual",
            "error",
            name="job_status",
        ),
        nullable=False,
        index=True,
    )
    date_created = db.Column(db.DateTime, index=True, default=func.now())
    date_finished = db.Column(db.DateTime)
    records_added = db.Column(db.Integer)
    records_updated = db.Column(db.Integer)
    records_deleted = db.Column(db.Integer)
    records_errored = db.Column(db.Integer)
    records_ignored = db.Column(db.Integer)
    errors = db.relationship(
        "HarvestJobError", backref="job", cascade="all, delete-orphan", lazy=True
    )
    records = db.relationship("HarvestRecord", backref="job", lazy=True)


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
    date_created = db.Column(db.DateTime, index=True, default=func.now())
    date_finished = db.Column(db.DateTime, index=True)
    ckan_id = db.Column(db.String, index=True)
    action = db.Column(
        Enum("create", "update", "delete", name="record_action"), index=True
    )
    status = db.Column(Enum("error", "success", name="record_status"), index=True)
    errors = db.relationship(
        "HarvestRecordError", backref="record", cascade="all, delete-orphan", lazy=True
    )


class HarvestJobError(Error):
    __tablename__ = "harvest_job_error"

    harvest_job_id = db.Column(
        db.String(36), db.ForeignKey("harvest_job.id"), nullable=False
    )


class HarvestRecordError(Error):
    __tablename__ = "harvest_record_error"

    harvest_record_id = db.Column(
        db.String, db.ForeignKey("harvest_record.id"), nullable=False
    )
