import os

import dotenv

from harvester.db.models import Base

from sqlalchemy import ForeignKey, SMALLINT, create_engine
from sqlalchemy import String, DateTime, Enum
from sqlalchemy.dialects.postgresql import JSON, UUID, ARRAY
from sqlalchemy.orm import mapped_column, sessionmaker
from sqlalchemy.sql import func

import enum

dotenv.load_dotenv()


class SeverityEnum(enum.Enum):
    error = "ERROR"
    critical = "CRITICAL"


class HarvestSource(Base):
    __tablename__ = "harvest_source"
    __table_args__ = {"comment": "Contains information for each harvest source"}

    name = mapped_column(String, nullable=False)
    notification_emails = mapped_column(ARRAY(String), nullable=False)
    organization_name = mapped_column(String, nullable=False)
    frequency = mapped_column(String, nullable=False)  # enum?
    config = mapped_column(JSON)
    urls = mapped_column(ARRAY(String), nullable=False)
    schema_validation_type = mapped_column(String, nullable=False)  # enum?


class HarvestJob(Base):
    __tablename__ = "harvest_job"
    __table_args__ = {
        "comment": "Contains job state information run through the pipeline"
    }

    source_id = mapped_column(UUID(as_uuid=True), ForeignKey("harvest_source.id"))
    status = mapped_column(String, nullable=False)  # enum?
    date_created = mapped_column(DateTime(timezone=True), server_default=func.now())
    date_finished = mapped_column(DateTime(timezone=True))
    extract_started = mapped_column(DateTime(timezone=True))
    extract_finished = mapped_column(DateTime(timezone=True))
    compare_started = mapped_column(DateTime(timezone=True))
    compare_finished = mapped_column(DateTime(timezone=True))
    records_added = mapped_column(SMALLINT)
    records_updated = mapped_column(SMALLINT)
    records_deleted = mapped_column(SMALLINT)
    records_errored = mapped_column(SMALLINT)
    records_ignored = mapped_column(SMALLINT)


class HarvestError(Base):
    __tablename__ = "harvest_error"
    __table_args__ = {"comment": "Table to contain all errors in the pipeline"}

    job_id = mapped_column(UUID(as_uuid=True), ForeignKey("harvest_job.id"))
    record_id = mapped_column(UUID(as_uuid=True))
    record_reported_id = mapped_column(String)
    date_created = mapped_column(DateTime(timezone=True), server_default=func.now())
    error_type = mapped_column(String)  # enum?
    severity = mapped_column(
        Enum(SeverityEnum, values_callable=lambda enum: [e.value for e in enum])
    )
    message = mapped_column(String)


class HarvestRecord(Base):
    __tablename__ = "harvest_record"
    __table_args__ = {"comment": "Table to contain records"}

    job_id = mapped_column(UUID(as_uuid=True), ForeignKey("harvest_job.id"))
    source_id = mapped_column(UUID(as_uuid=True), ForeignKey("harvest_source.id"))
    status = mapped_column(String)  # enum?
    s3_path = mapped_column(String)


def create_sqlalchemy_engine():
    host = os.getenv("POSTGRES_HOST")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")

    return create_engine(f"postgresql://{user}:{password}@{host}/{db}")


def create_sqlalchemy_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()
