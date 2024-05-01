import uuid

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Enum, func

db = SQLAlchemy()


class Base(db.Model):
    __abstract__ = True  # Indicates that this class should not be created as a table
    id = db.Column(db.String(36), primary_key=True, default=lambda: str(uuid.uuid4()))


class Organization(Base):
    __tablename__ = "organization"

    name = db.Column(db.String(), nullable=False, index=True)
    logo = db.Column(db.String())
    sources = db.relationship(
        "HarvestSource", backref="org", cascade="all, delete-orphan", lazy=True
    )


class HarvestSource(Base):
    __tablename__ = "harvest_source"

    name = db.Column(db.String, nullable=False)
    notification_emails = db.Column(db.String)
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


class HarvestJob(Base):
    __tablename__ = "harvest_job"

    harvest_source_id = db.Column(
        db.String(36), db.ForeignKey("harvest_source.id"), nullable=False
    )
    status = db.Column(
        Enum(
            "in_progress",
            "complete",
            "pending",
            "pending_manual",
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
        "HarvestError", backref="job", cascade="all, delete-orphan", lazy=True
    )


class HarvestError(Base):
    __tablename__ = "harvest_error"

    harvest_job_id = db.Column(
        db.String(36), db.ForeignKey("harvest_job.id"), nullable=False
    )
    harvest_record_id = db.Column(
        db.String, db.ForeignKey("harvest_record.id"), nullable=True
    )
    date_created = db.Column(db.DateTime, default=func.now())
    type = db.Column(db.String)
    severity = db.Column(
        Enum("CRITICAL", "ERROR", "WARN", name="error_serverity"),
        nullable=False,
        index=True,
    )
    message = db.Column(db.String)
    reference = db.Column(db.String)


class HarvestRecord(Base):
    __tablename__ = "harvest_record"

    identifier = db.Column(db.String())
    harvest_job_id = db.Column(
        db.String(36), db.ForeignKey("harvest_job.id"), nullable=True
    )
    harvest_source_id = db.Column(
        db.String(36), db.ForeignKey("harvest_source.id"), nullable=True
    )
    source_hash = db.Column(db.String)
    date_created = db.Column(db.DateTime, index=True, default=func.now())
    ckan_id = db.Column(db.String, index=True)
    type = db.Column(db.String)
    status = db.Column(db.String)
