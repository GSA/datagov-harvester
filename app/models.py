from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.sql import text
from sqlalchemy import Enum
from sqlalchemy.schema import UniqueConstraint

db = SQLAlchemy()

class Base(db.Model):
    __abstract__ = True  # Indicates that this class should not be created as a table
    id = db.Column(UUID(as_uuid=True), primary_key=True,
                   server_default=text("gen_random_uuid()"))

class Organization(Base):
    __tablename__ = "organization"
    
    name = db.Column(db.String(), nullable=False, index=True)
    logo = db.Column(db.String())
    sources = db.relationship("HarvestSource", backref="org",
                              cascade="all, delete-orphan",
                              lazy=True)

class HarvestSource(Base):
    __tablename__ = "harvest_source"
    
    name = db.Column(db.String, nullable=False)
    notification_emails = db.Column(db.String)
    organization_id = db.Column(UUID(as_uuid=True),
                                db.ForeignKey("organization.id"),
                                nullable=False)
    frequency = db.Column(db.String, nullable=False)
    url = db.Column(db.String, nullable=False, unique=True)
    schema_type = db.Column(db.String, nullable=False)
    source_type = db.Column(db.String, nullable=False)
    jobs = db.relationship("HarvestJob", backref="source",
                           cascade="all, delete-orphan",
                           lazy=True)

class HarvestJob(Base):
    __tablename__ = "harvest_job"
    
    harvest_source_id = db.Column(UUID(as_uuid=True),
                                  db.ForeignKey("harvest_source.id"),
                                  nullable=False)
    status = db.Column(Enum("new", "in_progress", "complete", name="job_status"),
                       nullable=False,
                       index=True)
    date_created = db.Column(db.DateTime, index=True)
    date_finished = db.Column(db.DateTime)
    records_added = db.Column(db.Integer)
    records_updated = db.Column(db.Integer)
    records_deleted = db.Column(db.Integer)
    records_errored = db.Column(db.Integer)
    records_ignored = db.Column(db.Integer)
    errors = db.relationship("HarvestError", backref="job",
                             cascade="all, delete-orphan",
                             lazy=True)

class HarvestError(Base):
    __tablename__ = "harvest_error"
    
    harvest_job_id = db.Column(UUID(as_uuid=True),
                               db.ForeignKey("harvest_job.id"),
                               nullable=False)
    harvest_record_id = db.Column(db.String)
    # to-do 
    # harvest_record_id = db.Column(UUID(as_uuid=True),
    #                               db.ForeignKey("harvest_record.id"),
    #                               nullable=True)
    date_created = db.Column(db.DateTime)
    type = db.Column(db.String)
    severity = db.Column(Enum("CRITICAL", "ERROR", "WARN", name="error_serverity"),
                         nullable=False,
                         index=True)
    message = db.Column(db.String)

class HarvestRecord(Base):
    __tablename__ = "harvest_record"
    
    job_id = db.Column(UUID(as_uuid=True),
                       db.ForeignKey("harvest_job.id"),
                       nullable=False) 
    identifier = db.Column(db.String(), nullable=False)
    ckan_id = db.Column(db.String(), nullable=False, index=True)
    type = db.Column(db.String(), nullable=False)
    source_metadata = db.Column(db.String(), nullable=True)
    __table_args__ = (
        UniqueConstraint("job_id", "identifier", name="uix_job_id_identifier"),
    )