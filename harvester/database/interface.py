from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from harvester.database.models import HarvestSource, HarvestJob, HarvestError
import os
from dotenv import load_dotenv

load_dotenv()
DATABASE_URI=os.getenv("DATABASE_URI")
engine = create_engine(DATABASE_URI)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class HarvesterDBInterface:
    def __init__(self):
        self.db = SessionLocal()
        
    def add_harvest_source(self, source_data):
        new_source = HarvestSource(**source_data)
        self.db.add(new_source)
        self.db.commit()
        self.db.refresh(new_source)
        return new_source

    def get_all_harvest_sources(self):
        def to_dict(source):
            return {
                'id': source.id,
                'name': source.name,
                'notification_emails': source.notification_emails,
                'organization_name': source.organization_name,
                'frequency': source.frequency,
                'config': source.config
            }

        harvest_sources = self.db.query(HarvestSource).all()
        harvest_sources_data = [to_dict(source) for source in harvest_sources]
        return harvest_sources_data
    
    def get_harvest_source(self, source_id):
        return self.db.query(HarvestSource).filter_by(id=source_id).first()

    def add_harvest_job(self, job_data):
        new_job = HarvestJob(**job_data)
        self.db.add(new_job)
        self.db.commit()
        self.db.refresh(new_job)
        return new_job

    def get_harvest_job(self, job_id):
        return self.db.query(HarvestJob).filter_by(id=job_id).first()

    def add_harvest_error(self, error_data):
        new_error = HarvestError(**error_data)
        self.db.add(new_error)
        self.db.commit()
        self.db.refresh(new_error)
        return new_error

    def get_harvest_error(self, error_id):
        return self.db.query(HarvestError).filter_by(id=error_id).first()

    def close(self):
        self.db.close()
