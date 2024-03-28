from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import NoResultFound, IntegrityError
from app.models import Organization, HarvestSource, HarvestJob, HarvestError
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
    
    def add_organization(self, org_data):
        try:
            new_org = Organization(**org_data)
            self.db.add(new_org)
            self.db.commit()
            self.db.refresh(new_org)
            return new_org
        except Exception as e:
            print("Error:", e)
            self.db.rollback()
            return None

    def get_all_organizations(self):
        orgs = self.db.query(Organization).all()
        orgs_data = [
            HarvesterDBInterface._to_dict(org) for org in orgs]
        return orgs_data
    
    def get_organization(self, org_id):
        result = self.db.query(Organization).filter_by(id=org_id).first()
        return HarvesterDBInterface._to_dict(result)

    def update_organization(self, org_id, updates):
        try:
            org = self.db.query(Organization).get(org_id)

            for key, value in updates.items():
                if hasattr(org, key):
                    setattr(org, key, value)
                else:
                    print(f"Warning: Trying to update non-existing field '{key}' in organization")

            self.db.commit()
            return self._to_dict(org)
        
        except NoResultFound:
            self.db.rollback()
            return None 
        
    def delete_organization(self, org_id):
        org = self.db.query(Organization).get(org_id)
        if org is None:
            return "Organization not found"
        self.db.delete(org)
        self.db.commit()
        return "Organization deleted successfully"
        
    def add_harvest_source(self, source_data):
        try:
            new_source = HarvestSource(**source_data)
            self.db.add(new_source)
            self.db.commit()
            self.db.refresh(new_source)
            return new_source
        except Exception as e:
            print("Error:", e)
            self.db.rollback()
            return None

    def get_all_harvest_sources(self):
        harvest_sources = self.db.query(HarvestSource).all()
        harvest_sources_data = [
            HarvesterDBInterface._to_dict(source) for source in harvest_sources]
        return harvest_sources_data
    
    def get_harvest_source(self, source_id):
        result = self.db.query(HarvestSource).filter_by(id=source_id).first()
        return HarvesterDBInterface._to_dict(result)
    
    def get_harvest_source_by_org(self, org_id):
        harvest_source = self.db.query(
            HarvestSource).filter_by(organization_id=org_id).all()
        harvest_source_data = [
            HarvesterDBInterface._to_dict(src) for src in harvest_source]
        return harvest_source_data

    def update_harvest_source(self, source_id, updates):
        try:
            source = self.db.query(HarvestSource).get(source_id)

            for key, value in updates.items():
                if hasattr(source, key):
                    setattr(source, key, value)
                else:
                    print(f"Warning: Trying to update non-existing field '{key}' in HarvestSource")

            self.db.commit()
            return self._to_dict(source)
        
        except NoResultFound:
            self.db.rollback()
            return None 
 
    def delete_harvest_source(self, source_id):
        source = self.db.query(HarvestSource).get(source_id)
        if source is None:
            return "Harvest source not found"
        self.db.delete(source)
        self.db.commit()
        return "Harvest source deleted successfully"
  
    def add_harvest_job(self, job_data):
        try:
            new_job = HarvestJob(**job_data)
            self.db.add(new_job)
            self.db.commit()
            self.db.refresh(new_job)
            return new_job
        except Exception as e:
            print("Error:", e)
            self.db.rollback()
            return None

    def get_all_harvest_jobs(self):
        harvest_jobs = self.db.query(HarvestJob).all()
        harvest_jobs_data = [
            HarvesterDBInterface._to_dict(job) for job in harvest_jobs]
        return harvest_jobs_data

    def get_harvest_job(self, job_id):
        result = self.db.query(HarvestJob).filter_by(id=job_id).first()
        return HarvesterDBInterface._to_dict(result)
    
    def get_harvest_job_by_source(self, source_id):
        harvest_job = self.db.query(
            HarvestJob).filter_by(harvest_source_id=source_id).all()
        harvest_job_data = [
            HarvesterDBInterface._to_dict(job) for job in harvest_job]
        return harvest_job_data

    def update_harvest_job(self, job_id, updates):
        try:
            job = self.db.query(HarvestJob).get(job_id)

            for key, value in updates.items():
                if hasattr(job, key):
                    setattr(job, key, value)
                else:
                    print(f"Warning: Trying to update non-existing field '{key}' in HavestJob")

            self.db.commit()
            return self._to_dict(job)
        
        except NoResultFound:
            self.db.rollback()
            return None 
        
    def delete_harvest_job(self, job_id):
        job = self.db.query(HarvestJob).get(job_id)
        if job is None:
            return "Harvest job not found"
        self.db.delete(job)
        self.db.commit()
        return "Harvest job deleted successfully"

    def add_harvest_error(self, error_data):
        try:
            new_error = HarvestError(**error_data)
            self.db.add(new_error)
            self.db.commit()
            self.db.refresh(new_error)
            return new_error
        except Exception as e:
            print("Error:", e)
            self.db.rollback()
            return None

    # for test, will remove later    
    def get_all_harvest_errors(self):
        harvest_errors = self.db.query(HarvestError).all()
        harvest_errors_data = [
            HarvesterDBInterface._to_dict(err) for err in harvest_errors]
        return harvest_errors_data

    def get_harvest_error_by_job(self, job_id):
        harvest_errors = self.db.query(HarvestError).filter_by(harvest_job_id=job_id)
        harvest_errors_data = [
            HarvesterDBInterface._to_dict(err) for err in harvest_errors]
        return harvest_errors_data

    def get_harvest_error(self, error_id):
        result = self.db.query(HarvestError).filter_by(id=error_id).first()
        return HarvesterDBInterface._to_dict(result)

    def close(self):
        if hasattr(self.db, "remove"):
            self.db.remove()
        elif hasattr(self.db, "close"):
            self.db.close()