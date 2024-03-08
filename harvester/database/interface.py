from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker, scoped_session
from harvester.database.models import HarvestSource, HarvestJob, HarvestError
from . import DATABASE_URI

class HarvesterDBInterface:
    def __init__(self, session=None):
        if session is None:
            engine = create_engine(DATABASE_URI)
            session_factory = sessionmaker(bind=engine,
                                           autocommit=False,
                                           autoflush=False)
            self.db = scoped_session(session_factory)
        else:
            self.db = session
        
    @staticmethod
    def _to_dict(obj):
        return {c.key: getattr(obj, c.key) 
                for c in inspect(obj).mapper.column_attrs}
    
    def add_harvest_source(self, source_data):
        new_source = HarvestSource(**source_data)
        self.db.add(new_source)
        self.db.commit()
        self.db.refresh(new_source)
        return new_source

    def get_all_harvest_sources(self):
        harvest_sources = self.db.query(HarvestSource).all()
        harvest_sources_data = [
            HarvesterDBInterface._to_dict(source) for source in harvest_sources]
        return harvest_sources_data
    
    def get_harvest_source(self, source_id):
        result = self.db.query(HarvestSource).filter_by(id=source_id).first()
        return HarvesterDBInterface._to_dict(result)

    def add_harvest_job(self, job_data, source_id):
        job_data['harvest_source_id'] = source_id
        new_job = HarvestJob(**job_data)
        self.db.add(new_job)
        self.db.commit()
        self.db.refresh(new_job)
        return new_job

    def get_all_harvest_jobs(self):
        harvest_jobs = self.db.query(HarvestJob).all()
        harvest_jobs_data = [
            HarvesterDBInterface._to_dict(job) for job in harvest_jobs]
        return harvest_jobs_data

    def get_harvest_job(self, job_id):
        result = self.db.query(HarvestJob).filter_by(id=job_id).first()
        return HarvesterDBInterface._to_dict(result)

    def add_harvest_error(self, error_data, job_id):
        error_data['harvest_job_id'] = job_id
        new_error = HarvestError(**error_data)
        self.db.add(new_error)
        self.db.commit()
        self.db.refresh(new_error)
        return new_error

    def get_all_harvest_errors_by_job(self, job_id):
        harvest_errors = self.db.query(HarvestError).filter_by(harvest_job_id=job_id)
        harvest_errors_data = [
            HarvesterDBInterface._to_dict(err) for err in harvest_errors]
        return harvest_errors_data

    def get_harvest_error(self, error_id):
        result = self.db.query(HarvestError).filter_by(id=error_id).first()
        return HarvesterDBInterface._to_dict(result)

    def close(self):
        if hasattr(self.db, 'remove'):
            self.db.remove()
        elif hasattr(self.db, 'close'):
            self.db.close()