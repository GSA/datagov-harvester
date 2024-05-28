import os
import uuid

from sqlalchemy import create_engine, inspect, or_, text
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import scoped_session, sessionmaker

from .models import HarvestError, HarvestJob, HarvestRecord, HarvestSource, Organization

DATABASE_URI = os.getenv("DATABASE_URI")


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
        if obj is None:
            return None
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
        return [HarvesterDBInterface._to_dict(org) for org in orgs]

    def get_organization(self, org_id):
        result = self.db.query(Organization).filter_by(id=org_id).first()
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
        return [HarvesterDBInterface._to_dict(source) for source in harvest_sources]

    def get_harvest_source(self, source_id):
        result = self.db.query(HarvestSource).filter_by(id=source_id).first()
        return HarvesterDBInterface._to_dict(result)

    def get_harvest_source_by_org(self, org_id):
        harvest_source = (
            self.db.query(HarvestSource).filter_by(organization_id=org_id).all()
        )
        return [HarvesterDBInterface._to_dict(src) for src in harvest_source]

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

    def get_harvest_job(self, job_id):
        result = self.db.query(HarvestJob).filter_by(id=job_id).first()
        return HarvesterDBInterface._to_dict(result)

    def get_harvest_jobs_by_filter(self, filter):
        harvest_jobs = self.db.query(HarvestJob).filter_by(**filter).all()
        harvest_jobs_data = [HarvesterDBInterface._to_dict(job) for job in harvest_jobs]
        return harvest_jobs_data

    def get_harvest_jobs_by_faceted_filter(self, attr, values):
        query_list = [getattr(HarvestJob, attr) == value for value in values]
        harvest_jobs = self.db.query(HarvestJob).filter(or_(*query_list)).all()
        harvest_jobs_data = [HarvesterDBInterface._to_dict(job) for job in harvest_jobs]
        return harvest_jobs_data

    def get_harvest_job_by_source(self, source_id):
        harvest_job = (
            self.db.query(HarvestJob).filter_by(harvest_source_id=source_id).all()
        )
        return [HarvesterDBInterface._to_dict(job) for job in harvest_job]

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

    def get_harvest_error(self, error_id):
        result = self.db.query(HarvestError).filter_by(id=error_id).first()
        return HarvesterDBInterface._to_dict(result)

    def get_harvest_errors_by_job(self, job_id: str) -> list:
        harvest_errors = self.db.query(HarvestError).filter_by(harvest_job_id=job_id)
        return [HarvesterDBInterface._to_dict(err) for err in harvest_errors]

    def get_harvest_errors_by_record_id(self, record_id: str) -> list:
        harvest_errors = self.db.query(HarvestError).filter_by(
            harvest_record_id=record_id
        )
        return [HarvesterDBInterface._to_dict(err) for err in harvest_errors]

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

    def add_harvest_records(self, records_data: list) -> dict:
        """
        Add many records at once

        :param list records_data: List of records with unique UUIDs
        :return dict id_lookup_table: identifiers -> ids
        :raises Exception: if the records_data contains records with errors
        """
        try:
            id_lookup_table = {}
            for i, record_data in enumerate(records_data):
                new_record = HarvestRecord(id=str(uuid.uuid4()), **record_data)
                id_lookup_table[new_record.identifier] = new_record.id
                self.db.add(new_record)
                if i % 1000 == 0:
                    self.db.flush()
            self.db.commit()
            return id_lookup_table
        except Exception as e:
            print("Error:", e)
            self.db.rollback()
            return None

    def update_harvest_record(self, record_id, updates):
        try:
            source = self.db.get(HarvestRecord, record_id)

            for key, value in updates.items():
                if hasattr(source, key):
                    setattr(source, key, value)
                else:
                    print(f"Warning: non-existing field '{key}' in HarvestRecord")

            self.db.commit()
            return self._to_dict(source)

        except NoResultFound:
            self.db.rollback()
            return None

    def get_harvest_record(self, record_id):
        result = self.db.query(HarvestRecord).filter_by(id=record_id).first()
        return HarvesterDBInterface._to_dict(result)

    def get_harvest_record_by_job(self, job_id):
        harvest_records = self.db.query(HarvestRecord).filter_by(harvest_job_id=job_id)
        return [HarvesterDBInterface._to_dict(rcd) for rcd in harvest_records]

    def get_harvest_record_by_source(self, source_id):
        harvest_records = self.db.query(HarvestRecord).filter_by(
            harvest_source_id=source_id
        )
        return [HarvesterDBInterface._to_dict(rcd) for rcd in harvest_records]

    def get_latest_records_by_source(self, source_id):
        # datetimes are returned as datetime objs not strs
        sql = text(
            f"""SELECT * FROM (
                SELECT DISTINCT ON (identifier) *
                FROM harvest_record
                WHERE status = 'success' AND harvest_source_id = '{source_id}'
                ORDER BY identifier, date_created DESC ) sq
                WHERE sq.action != 'delete';"""
        )

        res = self.db.execute(sql)

        fields = list(res.keys())
        records = res.fetchall()

        return [dict(zip(fields, record)) for record in records]

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

    ##### TEST INTERFACES BELOW #####
    ######## TO BE REMOVED ##########
    def get_all_harvest_jobs(self):
        harvest_jobs = self.db.query(HarvestJob).all()
        harvest_jobs_data = [HarvesterDBInterface._to_dict(job) for job in harvest_jobs]
        return harvest_jobs_data

    def get_all_harvest_records(self):
        harvest_records = self.db.query(HarvestRecord).all()
        harvest_records_data = [
            HarvesterDBInterface._to_dict(err) for err in harvest_records
        ]
        return harvest_records_data

    def get_all_harvest_errors(self):
        harvest_errors = self.db.query(HarvestError).all()
        harvest_errors_data = [
            HarvesterDBInterface._to_dict(err) for err in harvest_errors
        ]
        return harvest_errors_data
