from sqlalchemy import text, String, Integer, ForeignKey, ARRAY, DateTime
from sqlalchemy.orm import DeclarativeBase, relationship, mapped_column
from sqlalchemy.dialects.postgresql import JSON, UUID, ARRAY


class Base(DeclarativeBase):
    id = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )

class HarvestSource(Base):
    __tablename__ = 'harvest_source'
    
    name = mapped_column(String)
    notification_emails = mapped_column(ARRAY(String))
    organization_name = mapped_column(String)
    frequency = mapped_column(String)
    config = mapped_column(JSON) 

class HarvestJob(Base):
    __tablename__ = 'harvest_job'
    
    harvest_source_id = mapped_column(UUID(as_uuid=True), ForeignKey('harvest_source.id'), nullable=False)
    date_created = mapped_column(DateTime)
    date_finished = mapped_column(DateTime)
    records_added = mapped_column(Integer)
    records_updated = mapped_column(Integer)
    records_deleted = mapped_column(Integer)
    records_errored = mapped_column(Integer)
    records_ignored = mapped_column(Integer)
    
    source = relationship("HarvestSource", back_populates="jobs")

class HarvestError(Base):
    __tablename__ = 'harvest_error'
    
    harvest_job_id = mapped_column(UUID(as_uuid=True), ForeignKey('harvest_job.id'), nullable=False)
    record_id = mapped_column(String, nullable=True)
    record_reported_id = mapped_column(String)
    date_created = mapped_column(DateTime)
    type = mapped_column(String)
    severity = mapped_column(String)
    message = mapped_column(String)
    
    job = relationship("HarvestJob", back_populates="errors")

HarvestSource.jobs = relationship("HarvestJob", order_by=HarvestJob.id, back_populates="source")
HarvestJob.errors = relationship("HarvestError", order_by=HarvestError.id, back_populates="job")
