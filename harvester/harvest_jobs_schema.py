from sqlalchemy import INTEGER, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TEXT

Base = declarative_base()


class HarvestJob(Base):
    __tablename__ = "harvestjob"

    ID = Column("id", INTEGER, primary_key=True)
    JobID = Column("jobid", TEXT())
    Status = Column("status", TEXT(), nullable=False)

    def __repr__(self):
        return f"<HarvestJob(JobID={self.JobID},Status={self.Status})>"
