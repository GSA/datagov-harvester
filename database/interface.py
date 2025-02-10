import logging
import os
import uuid
from datetime import datetime, timezone
from functools import wraps

from sqlalchemy import create_engine, func, inspect, select, text
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import scoped_session, sessionmaker

from harvester.utils.general_utils import query_filter_builder

from .models import (
    HarvestJob,
    HarvestJobError,
    HarvestRecord,
    HarvestRecordError,
    HarvestSource,
    HarvestUser,
    Organization,
)

DATABASE_URI = os.getenv("DATABASE_URI")
PAGINATE_ENTRIES_PER_PAGE = 20
PAGINATE_START_PAGE = 0


def paginate(fn):
    @wraps(fn)
    def _impl(self, *args, **kwargs):
        query = fn(self, *args, **kwargs)
        if kwargs.get("count") is True:
            return query
        elif kwargs.get("paginate") is False:
            return query.all()
        else:
            per_page = kwargs.get("per_page") or PAGINATE_ENTRIES_PER_PAGE
            page = kwargs.get("page") or PAGINATE_START_PAGE
            query = query.limit(per_page)
            query = query.offset(page * per_page)
            return query.all()

    return _impl


# notes on the flag `maintain_column_froms`:
# https://github.com/sqlalchemy/sqlalchemy/discussions/6807#discussioncomment-1043732
# docs: https://docs.sqlalchemy.org/en/14/core/selectable.html#sqlalchemy.sql.expression.Select.with_only_columns.params.maintain_column_froms
#
def count(fn):
    @wraps(fn)
    def _impl(self, *args, **kwargs):
        query = fn(self, *args, **kwargs)
        if kwargs.get("count") is True:
            count_q = query.statement.with_only_columns(
                func.count(), maintain_column_froms=True
            ).order_by(None)
            count = query.session.execute(count_q).scalar()
            return count
        else:
            return query

    return _impl


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


class HarvesterDBInterface:
    def __init__(self, session=None):
        if session is None:
            engine = create_engine(
                DATABASE_URI,
                isolation_level="AUTOCOMMIT",
                pool_size=10,
                max_overflow=20,
                pool_timeout=60,
                pool_recycle=1800,
            )
            session_factory = sessionmaker(bind=engine, autoflush=True)
            self.db = scoped_session(session_factory)
        else:
            self.db = session

    @staticmethod
    def _to_dict(obj):
        def to_dict_helper(obj):
            return {
                c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs
            }

        if isinstance(obj, list):
            return [to_dict_helper(x) for x in obj]
        else:
            return to_dict_helper(obj)

    @staticmethod
    def _to_list(obj):
        def to_list_helper(obj):
            return [getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs]

        if isinstance(obj, list):
            return [to_list_helper(x) for x in obj]
        else:
            return to_list_helper(obj)

    ## ORGANIZATIONS
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

    def get_organization(self, org_id):
        return self.db.query(Organization).filter_by(id=org_id).first()

    def get_all_organizations(self):
        orgs = self.db.query(Organization).all()
        return [org for org in orgs]

    def update_organization(self, org_id, updates):
        try:
            org = self.db.get(Organization, org_id)

            for key, value in updates.items():
                if hasattr(org, key):
                    setattr(org, key, value)
                else:
                    print(f"Warning: non-existing field '{key}' in organization")

            self.db.commit()
            return org

        except NoResultFound:
            self.db.rollback()
            return None

    def delete_organization(self, org_id):
        org = self.db.get(Organization, org_id)
        if org is None:
            return None
        self.db.delete(org)
        self.db.commit()
        return "Organization deleted successfully"

    ## HARVEST SOURCES
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

    def get_harvest_source(self, source_id):
        result = self.db.query(HarvestSource).filter_by(id=source_id).first()
        return result

    def get_harvest_source_by_org(self, org_id):
        harvest_source = (
            self.db.query(HarvestSource).filter_by(organization_id=org_id).all()
        )
        return [src for src in harvest_source]

    def get_harvest_source_by_jobid(self, jobid):
        harvest_job = self.db.query(HarvestJob).filter_by(id=jobid).first()
        if harvest_job is None:
            return None
        else:
            return harvest_job.source

    def get_all_harvest_sources(self):
        harvest_sources = self.db.query(HarvestSource).all()
        return [source for source in harvest_sources]

    def update_harvest_source(self, source_id, updates):
        try:
            source = self.db.get(HarvestSource, source_id)
            for key, value in updates.items():
                if hasattr(source, key):
                    setattr(source, key, value)
                else:
                    print(f"Warning: non-existing field '{key}' in HarvestSource")
            self.db.commit()
            return source

        except NoResultFound:
            self.db.rollback()
            return None

    def delete_harvest_source(self, source_id):
        source = self.db.get(HarvestSource, source_id)
        if source is None:
            return "Harvest source not found"

        records = (
            self.db.query(HarvestRecord)
            .filter(
                HarvestRecord.harvest_source_id == source_id,
                HarvestRecord.ckan_id.isnot(None),
            )
            .all()
        )

        if len(records) == 0:
            self.db.delete(source)
            self.db.commit()
            return "Harvest source deleted successfully"
        else:
            return (
                f"Failed: {len(records)} records in the Harvest source, "
                "please Clear it first."
            )

    ## HARVEST JOB
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
        return self.db.query(HarvestJob).filter_by(id=job_id).first()

    def get_first_harvest_job_by_filter(self, filter):
        harvest_job = (
            self.db.query(HarvestJob)
            .filter_by(**filter)
            .order_by(HarvestJob.date_created.desc())
            .first()
        )
        return harvest_job

    def get_harvest_jobs_by_source_id(self, source_id):
        """used by follow-up job helper"""
        harvest_jobs = (
            self.db.query(HarvestJob)
            .filter_by(harvest_source_id=source_id)
            .order_by(HarvestJob.date_created.desc())
            .limit(2)
            .all()
        )
        return harvest_jobs

    def get_new_harvest_jobs_in_past(self):
        harvest_jobs = (
            self.db.query(HarvestJob)
            .filter(
                HarvestJob.date_created < datetime.now(timezone.utc),
                HarvestJob.status == "new",
            )
            .all()
        )
        return [job for job in harvest_jobs]

    def get_new_harvest_jobs_by_source_in_future(self, source_id):
        harvest_jobs = (
            self.db.query(HarvestJob)
            .filter(
                HarvestJob.date_created > datetime.now(timezone.utc),
                HarvestJob.harvest_source_id == source_id,
                HarvestJob.status == "new",
            )
            .all()
        )
        return [job for job in harvest_jobs or []]

    def update_harvest_job(self, job_id, updates):
        try:
            job = self.db.get(HarvestJob, job_id)

            for key, value in updates.items():
                if hasattr(job, key):
                    setattr(job, key, value)
                else:
                    print(f"Warning: non-existing field '{key}' in HarvestJob")

            self.db.commit()
            return job

        except NoResultFound:
            self.db.rollback()
            return None

    def delete_harvest_job(self, job_id):
        job = self.db.get(HarvestJob, job_id)
        if job is None:
            return f"Harvest job {job_id} not found"
        self.db.delete(job)
        self.db.commit()
        return "Harvest job deleted successfully"

    ## HARVEST ERROR
    def add_harvest_job_error(self, error_data: dict):
        try:
            new_error = HarvestJobError(**error_data)
            self.db.add(new_error)
            self.db.commit()
            self.db.refresh(new_error)
            return new_error
        except Exception as e:
            print("Error:", e)
            self.db.rollback()
            return None

    def add_harvest_record_error(self, error_data: dict):
        try:
            new_error = HarvestRecordError(**error_data)
            self.db.add(new_error)
            self.db.commit()
            self.db.refresh(new_error)
            return new_error
        except Exception as e:
            print("Error:", e)
            self.db.rollback()
            return None

    def get_harvest_job_errors_by_job(self, job_id: str) -> list[dict]:
        job = self.get_harvest_job(job_id)
        return [error for error in job.errors or []]

    @count
    @paginate
    def get_harvest_record_errors_by_job(self, job_id: str, **kwargs):
        """
            Retrieves harvest record errors for a given job.

            This function fetches all records where the harvest status is 'error' and
            belongs to the specified job. The query returns a tuple containing:
                - HarvestRecordError object
                - identifier (retrieved from HarvestRecord)
                - source_raw (retrieved from HarvestRecord, containing 'title')

            Returns:
                Query: A SQLAlchemy Query object that, when executed, yields tuples of:
                    (HarvestRecordError, identifier, source_raw).
        """
        subquery = (
            self.db.query(HarvestRecord.id)
            .filter(HarvestRecord.status == "error")
            .filter(HarvestRecord.harvest_job_id == job_id)
            .subquery()
        )
        query = (
            self.db.query(
                HarvestRecordError,
                HarvestRecord.identifier,
                HarvestRecord.source_raw
            )
            .join(HarvestRecord, 
                HarvestRecord.id == HarvestRecordError.harvest_record_id)
            .filter(HarvestRecord.id.in_(select(subquery)))
        )
        return query

    def get_harvest_error(self, error_id: str) -> dict:
        job_query = self.db.query(HarvestJobError).filter_by(id=error_id).first()
        record_query = self.db.query(HarvestRecordError).filter_by(id=error_id).first()
        if job_query is not None:
            return job_query
        elif record_query is not None:
            return record_query
        else:
            return None

    def get_harvest_record_errors_by_record(self, record_id: str):
        errors = self.db.query(HarvestRecordError).filter_by(
            harvest_record_id=record_id
        )
        return [err for err in errors or []]

    ## HARVEST RECORD
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

    # TODO: should we delete this if it's not used in code.
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
                    logger.error(f"Non-existing field '{key}' in HarvestRecord")

            self.db.commit()
            return source

        except NoResultFound:
            self.db.rollback()
            return None

    def delete_harvest_record(self, identifier):
        records = self.db.query(HarvestRecord).filter_by(identifier=identifier).all()
        if not records:
            logger.warning(f"Harvest records with identifier {identifier} not found")
            return
        logger.info(
            f"{len(records)} records with idenitifier {identifier}\
                  found in datagov-harvest-db"
        )
        for record in records:
            self.db.delete(record)
        self.db.commit()
        return "Harvest record deleted successfully"

    def get_harvest_record(self, record_id):
        return self.db.query(HarvestRecord).filter_by(id=record_id).first()

    def get_latest_harvest_records_by_source(self, source_id):
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

    def get_all_latest_harvest_records_by_source(self, source_id):
        """used by follow-up job helper"""
        # datetimes are returned as datetime objs not strs
        sql = text(
            f"""SELECT DISTINCT ON (identifier) *
                FROM harvest_record
                WHERE harvest_source_id = '{source_id}'
                ORDER BY identifier, date_created DESC"""
        )

        res = self.db.execute(sql)

        fields = list(res.keys())
        records = res.fetchall()

        return [dict(zip(fields, record)) for record in records]

    def close(self):
        if hasattr(self.db, "remove"):
            self.db.remove()
        elif hasattr(self.db, "close"):
            self.db.close()

    # User management
    def add_user(self, usr_data):
        try:
            if not usr_data["email"].endswith(".gov"):
                return False, "Error: Email address must be a .gov address."

            existing_user = (
                self.db.query(HarvestUser).filter_by(email=usr_data["email"]).first()
            )

            if existing_user:
                return False, "User with this email already exists."

            new_user = HarvestUser(**usr_data)
            self.db.add(new_user)
            self.db.commit()
            self.db.refresh(new_user)
            return True, new_user
        except Exception as e:
            print("Error:", e)
            self.db.rollback()
            return False, "An error occurred while adding the user."

    def list_users(self):
        try:
            return self.db.query(HarvestUser).all()
        except Exception as e:
            print("Error:", e)
            return []

    def remove_user(self, email):
        try:
            user = self.db.query(HarvestUser).filter_by(email=email).first()
            if user:
                self.db.delete(user)
                self.db.commit()
                return True
            return False
        except Exception as e:
            print("Error:", e)
            self.db.rollback()
            return False

    def verify_user(self, usr_data):
        try:
            user_by_ssoid = (
                self.db.query(HarvestUser).filter_by(ssoid=usr_data["ssoid"]).first()
            )
            if user_by_ssoid:
                if user_by_ssoid.email == usr_data["email"]:
                    return True
                else:
                    return False
            else:
                user_by_email = (
                    self.db.query(HarvestUser)
                    .filter_by(email=usr_data["email"])
                    .first()
                )
                if user_by_email:
                    user_by_email.ssoid = usr_data["ssoid"]
                    self.db.commit()
                    self.db.refresh(user_by_email)
                    return True
            return False
        except Exception as e:
            print("Error:", e)
            return False

    #### PAGINATED QUERIES ####
    @count
    @paginate
    def pget_harvest_jobs(self, facets="", **kwargs):
        facet_string = query_filter_builder(None, facets)
        return self.db.query(HarvestJob).filter(text(facet_string))

    @count
    @paginate
    def pget_harvest_records(self, facets="", **kwargs):
        facet_string = query_filter_builder(None, facets)
        return self.db.query(HarvestRecord).filter(text(facet_string))

    @count
    @paginate
    def pget_harvest_job_errors(self, facets="", **kwargs):
        facet_string = query_filter_builder(None, facets)
        return self.db.query(HarvestJobError).filter(text(facet_string))

    @count
    @paginate
    def pget_harvest_record_errors(self, facets="", **kwargs):
        facet_string = query_filter_builder(None, facets)
        return self.db.query(HarvestRecordError).filter(text(facet_string))

    #### FILTERED BUILDER QUERIES ####
    def get_harvest_records_by_job(self, job_id, facets="", **kwargs):
        facet_string = query_filter_builder(f"harvest_job_id = '{job_id}'", facets)
        return self.pget_harvest_records(facets=facet_string, **kwargs)

    def get_harvest_records_by_source(self, source_id, facets="", **kwargs):
        facet_string = query_filter_builder(
            f"harvest_source_id = '{source_id}'", facets
        )
        return self.pget_harvest_records(facets=facet_string, **kwargs)
