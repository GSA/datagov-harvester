from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import scoped_session, sessionmaker

from app.models import (
    HarvestError,
    HarvestJob,
    HarvestRecord,
    HarvestSource,
    Organization,
)

from . import DATABASE_URI


class HarvesterDBInterface:
    def __init__(self, session=None):
        if session is None:
            engine = create_engine(
                DATABASE_URI,
                pool_size=10,
                max_overflow=20,
                pool_timeout=60,
                pool_recycle=1800,
            )
            session_factory = sessionmaker(
                bind=engine, autocommit=False, autoflush=False
            )
            self.db = scoped_session(session_factory)
        else:
            self.db = session

    @staticmethod
    def _to_dict(obj):
        return {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}

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
        if orgs is None:
            return None
        else:
            orgs_data = [HarvesterDBInterface._to_dict(org) for org in orgs]
            return orgs_data

    def get_organization(self, org_id):
        result = self.db.query(Organization).filter_by(id=org_id).first()
        if result is None:
            return None
        return HarvesterDBInterface._to_dict(result)

    def update_organization(self, org_id, updates):
        try:
            org = self.db.get(Organization, org_id)

            for key, value in updates.items():
                if hasattr(org, key):
                    setattr(org, key, value)
                else:
                    print(f"Warning: non-existing field '{key}' in organization")

            self.db.commit()
            return self._to_dict(org)

        except NoResultFound:
            self.db.rollback()
            return None

    def delete_organization(self, org_id):
        org = self.db.get(Organization, org_id)
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
        if harvest_sources is None:
            return None
        else:
            harvest_sources_data = [
                HarvesterDBInterface._to_dict(source) for source in harvest_sources
            ]
            return harvest_sources_data

    def get_harvest_source(self, source_id):
        result = self.db.query(HarvestSource).filter_by(id=source_id).first()
        if result is None:
            return None
        return HarvesterDBInterface._to_dict(result)

    def get_harvest_source_by_org(self, org_id):
        harvest_source = (
            self.db.query(HarvestSource).filter_by(organization_id=org_id).all()
        )
        if harvest_source is None:
            return None
        else:
            harvest_source_data = [
                HarvesterDBInterface._to_dict(src) for src in harvest_source
            ]
            return harvest_source_data

    def update_harvest_source(self, source_id, updates):
        try:
            source = self.db.get(HarvestSource, source_id)

            for key, value in updates.items():
                if hasattr(source, key):
                    setattr(source, key, value)
                else:
                    print(f"Warning: non-existing field '{key}' in HarvestSource")

            self.db.commit()
            return self._to_dict(source)

        except NoResultFound:
            self.db.rollback()
            return None

    def delete_harvest_source(self, source_id):
        source = self.db.get(HarvestSource, source_id)
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

    # for test, will remove later
    def get_all_harvest_jobs(self):
        harvest_jobs = self.db.query(HarvestJob).all()
        harvest_jobs_data = [HarvesterDBInterface._to_dict(job) for job in harvest_jobs]
        return harvest_jobs_data

    def get_harvest_job(self, job_id):
        result = self.db.query(HarvestJob).filter_by(id=job_id).first()
        if result is None:
            return None
        return HarvesterDBInterface._to_dict(result)

    def get_harvest_job_by_source(self, source_id):
        harvest_job = (
            self.db.query(HarvestJob).filter_by(harvest_source_id=source_id).all()
        )
        if harvest_job is None:
            return None
        else:
            harvest_job_data = [
                HarvesterDBInterface._to_dict(job) for job in harvest_job
            ]
            return harvest_job_data

    def update_harvest_job(self, job_id, updates):
        try:
            job = self.db.get(HarvestJob, job_id)

            for key, value in updates.items():
                if hasattr(job, key):
                    setattr(job, key, value)
                else:
                    print(f"Warning: non-existing field '{key}' in HavestJob")

            self.db.commit()
            return self._to_dict(job)

        except NoResultFound:
            self.db.rollback()
            return None

    def delete_harvest_job(self, job_id):
        job = self.db.get(HarvestJob, job_id)
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
            HarvesterDBInterface._to_dict(err) for err in harvest_errors
        ]
        return harvest_errors_data

    def get_harvest_error(self, error_id):
        result = self.db.query(HarvestError).filter_by(id=error_id).first()
        if result is None:
            return None
        return HarvesterDBInterface._to_dict(result)

    def get_harvest_error_by_job(self, job_id):
        harvest_errors = self.db.query(HarvestError).filter_by(harvest_job_id=job_id)
        if harvest_errors is None:
            return None
        else:
            harvest_errors_data = [
                HarvesterDBInterface._to_dict(err) for err in harvest_errors
            ]
            return harvest_errors_data

    def add_harvest_record(self, record_data):
        try:
            new_record = HarvestRecord(**record_data)
            self.db.add(new_record)
            self.db.commit()
            self.db.refresh(new_record)
            return new_record
        except Exception as e:
            print("Error:", e)
            self.db.rollback()
            return None

    # for test, will remove later
    def get_all_harvest_records(self):
        harvest_records = self.db.query(HarvestRecord).all()
        harvest_records_data = [
            HarvesterDBInterface._to_dict(err) for err in harvest_records
        ]
        return harvest_records_data

    def get_harvest_record(self, record_id):
        result = self.db.query(HarvestRecord).filter_by(id=record_id).first()
        if result is None:
            return None
        return HarvesterDBInterface._to_dict(result)

    def get_harvest_record_by_job(self, job_id):
        harvest_records = self.db.query(HarvestRecord).filter_by(harvest_job_id=job_id)
        if harvest_records is None:
            return None
        else:
            harvest_records_data = [
                HarvesterDBInterface._to_dict(rcd) for rcd in harvest_records
            ]
            return harvest_records_data

    def get_harvest_record_by_source(self, source_id):
        harvest_records = self.db.query(HarvestRecord).filter_by(
            harvest_source_id=source_id
        )
        if harvest_records is None:
            return None
        else:
            harvest_records_data = [
                HarvesterDBInterface._to_dict(rcd) for rcd in harvest_records
            ]
            return harvest_records_data

    def get_source_by_jobid(self, jobid):
        harvest_job = self.db.query(HarvestJob).filter_by(id=jobid).first()
        if harvest_job is None:
            return None
        else:
            return HarvesterDBInterface._to_dict(harvest_job.source)

    def close(self):
        if hasattr(self.db, "remove"):
            self.db.remove()
        elif hasattr(self.db, "close"):
            self.db.close()
