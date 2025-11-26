import logging
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import List, Optional

from sqlalchemy import asc, desc, func, inspect, text
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import aliased
from sqlalchemy.dialects.postgresql import insert

from harvester.utils.general_utils import query_filter_builder

from .models import (
    Dataset,
    DatasetViewCount,
    HarvestJob,
    HarvestJobError,
    HarvestRecord,
    HarvestRecordError,
    HarvestSource,
    HarvestUser,
    Locations,
    Organization,
    db,
)

PAGINATE_ENTRIES_PER_PAGE = 10
PAGINATE_START_PAGE = 0

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


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
            return query.session.execute(count_q).scalar()
        else:
            return query

    return _impl


def count_wrapper(fn):
    """A wrapper that enables non-paginated functions to use the count decorater"""

    @wraps(fn)
    def _impl(self, *args, **kwargs):
        query = fn(self, *args, **kwargs)
        if kwargs.get("count") is True:
            return query
        else:
            return query.all()

    return _impl


class HarvesterDBInterface:
    def __init__(self, session=None):
        if session is not None:
            ## For the Harvest Runner we create our own session and pass it in
            self.db = session
        else:
            # Flask-SQLAlchemy provides a request-scoped database session
            # so use it here
            self.db = db.session

    @staticmethod
    def _to_dict(obj):
        if obj is None:
            return {}

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
            logger.error("Error: %s", e)
            self.db.rollback()
            return None

    def get_organization(self, org_id):
        return self.db.query(Organization).filter_by(id=org_id).first()

    def get_all_organizations(self):
        orgs = self.db.query(Organization).all()
        return [org for org in orgs]

    def get_organization_by_slug(self, slug):
        return self.db.query(Organization).filter(Organization.slug == slug).first()

    def update_organization(self, org_id, updates):
        try:
            org = self.db.get(Organization, org_id)

            for key, value in updates.items():
                if hasattr(org, key):
                    setattr(org, key, value)
                else:
                    logger.warning(
                        "Warning: non-existing field '%s' in organization", key
                    )

            self.db.commit()
            return org

        except NoResultFound:
            self.db.rollback()
            return None

    def delete_organization(self, org_id):
        org = self.db.get(Organization, org_id)
        if org is None:
            return None

        harvest_sources = (
            self.db.query(HarvestSource)
            .filter(HarvestSource.organization_id == org_id)
            .all()
        )

        if len(harvest_sources) == 0:
            self.db.delete(org)
            self.db.commit()
            return (f"Deleted organization with ID:{org_id} successfully", 200)
        else:
            # ruff: noqa: E501
            return (
                f"Failed: {len(harvest_sources)} harvest sources in the organization, please delete those first.",
                409,
            )

    ## HARVEST SOURCES
    def add_harvest_source(self, source_data):
        try:
            new_source = HarvestSource(**source_data)
            self.db.add(new_source)
            self.db.commit()
            self.db.refresh(new_source)
            return new_source
        except Exception as e:
            logger.error("Error: %s", e)
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
                    logger.warning(
                        "Warning: non-existing field '%s' in HarvestSource", key
                    )
            self.db.commit()
            return source

        except NoResultFound:
            self.db.rollback()
            return None

    def delete_harvest_source(self, source_id):
        source = self.db.get(HarvestSource, source_id)
        if source is None:
            return "Harvest source not found"

        record_count = self.get_latest_harvest_records_by_source_orm(
            source_id, synced=True, count=True
        )

        if record_count == 0:
            self.db.delete(source)
            self.db.commit()
            return (
                f"Deleted harvest source with ID:{source_id} successfully",
                200,
            )
        else:
            # ruff: noqa: E501
            return (
                f"Failed: {record_count} records in the Harvest source, please clear it first.",
                409,
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
            logger.error("Error: %s", e)
            self.db.rollback()
            return None

    def get_harvest_job(self, job_id):
        return self.db.query(HarvestJob).filter_by(id=job_id).first()

    def get_orphaned_harvest_jobs(self) -> List[HarvestJob]:
        """
        Retrieves all harvest jobs that are in progress and
        were created more than one day ago.
        """
        one_day_ago = datetime.now(timezone.utc) - timedelta(days=1)
        return (
            self.db.query(HarvestJob)
            .filter(
                HarvestJob.status == "in_progress",
                HarvestJob.date_created <= one_day_ago,
            )
            .order_by(HarvestJob.date_created.desc())
            .all()
        )

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

    def get_in_progress_jobs(self):
        """Get harvest jobs that are in progress."""
        return list(
            self.db.query(HarvestJob).filter(HarvestJob.status == "in_progress")
        )

    def get_new_harvest_jobs_in_past(self, limit=None):
        """Get harvest jobs in the database that need to be run.

        A job that needs to be run has status "new" and a date_created that is
        before now. The jobs are returned in ascending order of date_created so that the
        oldest jobs are given first.

        If `limit` is given, it limits the number of returned jobs to at most that
        number. The default is to return all the jobs.
        """
        return (
            self.db.query(HarvestJob)
            .filter(
                HarvestJob.date_created < datetime.now(timezone.utc),
                HarvestJob.status == "new",
            )
            .order_by(asc(HarvestJob.date_created))
            .limit(limit)
            .all()
        )

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
                    logger.warning(
                        "Warning: non-existing field '%s' in HarvestJob", key
                    )

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
        return "Harvest job deleted successfully", 200

    ## HARVEST ERROR
    def add_harvest_job_error(self, error_data: dict):
        try:
            new_error = HarvestJobError(**error_data)
            self.db.add(new_error)
            self.db.commit()
            self.db.refresh(new_error)
            return new_error
        except Exception as e:
            logger.error("Error: %s", e)
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
            logger.error("Error: %s", e)
            self.db.rollback()
            return None

    def get_harvest_job_errors_by_job(self, job_id: str) -> list[dict]:
        job = self.get_harvest_job(job_id)
        return [error for error in job.errors or []]

    def get_harvest_record_errors_by_job_for_view(self, query):
        """
        groups validation messages based on harvest record id. aggregates the error
        messages into a comma-separated string. for all other fields, the 1st row in the group
        is used.
        """
        query = query.subquery()

        error_types = ["ValidationException", "ValidationError"]

        # aggregate the validation messages by harvest_record_id using 1st row for
        # all other fields. sql indexing starts at 1
        instance_idx = 1
        agg = (
            self.db.query(
                func.array_agg(query.c.harvest_record_id)[instance_idx],
                func.array_agg(query.c.harvest_job_id)[instance_idx],
                func.array_agg(query.c.date_created)[instance_idx],
                func.array_agg(query.c.type)[instance_idx],
                func.array_to_string(func.array_agg(query.c.message), "::"),
                func.array_agg(query.c.id)[instance_idx],
                func.array_agg(query.c.identifier)[instance_idx],
                func.array_agg(query.c.source_raw)[instance_idx],
            )
            .filter(query.c.type.in_(error_types))
            .group_by(query.c.harvest_record_id)
        )

        # get all other messages as-is and combine
        other = self.db.query(query).filter(query.c.type.not_in(error_types))

        return agg.union(other)

    @count
    @paginate
    def get_harvest_record_errors_by_job(self, job_id: str, **kwargs):
        """
        Retrieves harvest record errors for a given job.

        This function fetches all harvest record errors that belong to a specified job,
        and joins them with their harvest record, if one exists.
        The query returns a tuple containing:
            - HarvestRecordError object
            - identifier (retrieved from HarvestRecord, can be None)
            - source_raw (retrieved from HarvestRecord, containing 'title', can be None)

        Or a tuple containing:
            - Harvest record error values (not as a HarvestRecordError object)
            - identifier (retrieved from HarvestRecord, can be None)
            - source_raw (retrieved from HarvestRecord, containing 'title', can be None)

        Returns:
            Query: A SQLAlchemy Query object that, when executed, yields tuples of:
                (HarvestRecordError, identifier, source_raw).
        """
        query = (
            self.db.query(
                HarvestRecordError, HarvestRecord.identifier, HarvestRecord.source_raw
            )
            .outerjoin(
                HarvestRecord, HarvestRecord.id == HarvestRecordError.harvest_record_id
            )
            .filter(HarvestRecordError.harvest_job_id == job_id)
        )

        for_view = kwargs.get("for_view", False)
        if for_view:
            return self.get_harvest_record_errors_by_job_for_view(query)

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

    def get_record_errors_summary_by_job(self, job_id: str):
        """Get a summary of all the record errors for this job."""
        query = (
            self.db.query(HarvestRecordError.type, func.count())
            .where(HarvestRecordError.harvest_job_id == job_id)
            .group_by(HarvestRecordError.type)
        )
        return {error_type: error_count for error_type, error_count in query}

    ## HARVEST RECORD
    def add_harvest_record(self, record_data):
        try:
            new_record = HarvestRecord(**record_data)
            self.db.add(new_record)
            self.db.commit()
            self.db.refresh(new_record)
            return new_record
        except Exception as e:
            logger.error("Write to db error: %s", e)
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

    def delete_harvest_record(
        self, identifier=None, record_id=None, harvest_source_id=None
    ):
        try:
            # delete all versions of the record within the given harvest source
            if harvest_source_id is not None and identifier is not None:
                records = (
                    self.db.query(HarvestRecord)
                    .filter_by(
                        identifier=identifier, harvest_source_id=harvest_source_id
                    )
                    .all()
                )
            # delete this exact one (used with cleaning)
            if record_id is not None:
                records = self.db.query(HarvestRecord).filter_by(id=record_id).all()
            if len(records) == 0:
                logger.warning(
                    f"Harvest records with identifier {identifier} or {record_id} "
                    "not found"
                )
                return
            logger.info(
                f"{len(records)} records with identifier {identifier} or {record_id}\
                  found in datagov-harvest-db"
            )
            for record in records:
                self.db.delete(record)
            self.db.commit()
            return "Harvest record deleted successfully"
        except:  # noqa E722
            self.db.rollback()
            return None

    def get_harvest_record(self, record_id):
        return self.db.query(HarvestRecord).filter_by(id=record_id).first()

    ## DATASETS
    def _apply_popularity_from_view_count(self, dataset: Optional[Dataset]) -> None:
        if dataset is None or not dataset.slug:
            return

        view_count = (
            self.db.query(DatasetViewCount.view_count)
            .filter(DatasetViewCount.dataset_slug == dataset.slug)
            .scalar()
        )
        if view_count is None:
            return

        dataset.popularity = view_count

    def insert_dataset(self, dataset_data: dict):
        slug = dataset_data.get("slug")
        if not slug:
            raise ValueError("dataset_data must include a slug")

        # use a nested transaction so that rollbacks don't rollback
        # the whole session during the pytests and fail
        nested = self.db.begin_nested()
        try:
            dataset = Dataset(**dataset_data)
            self.db.add(dataset)
            self.db.flush()
            self._apply_popularity_from_view_count(dataset)
        except Exception as e:
            nested.rollback()
            logger.error("Error inserting dataset '%s': %s", slug, e)
            raise
        else:
            nested.commit()
            try:
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
            self.db.refresh(dataset)
            return dataset

    def upsert_dataset(self, dataset_data: dict):
        slug = dataset_data.get("slug")
        if not slug:
            raise ValueError("dataset_data must include a slug")

        stmt = insert(Dataset).values(**dataset_data)
        update_cols = {
            column: getattr(stmt.excluded, column)
            for column in dataset_data
            if column != "slug"
        }
        stmt = stmt.on_conflict_do_update(
            index_elements=[Dataset.slug],
            set_=update_cols,
        )

        nested = self.db.begin_nested()
        dataset = None
        try:
            self.db.execute(stmt)
            self.db.flush()
            dataset = self.get_dataset_by_slug(slug)
            self._apply_popularity_from_view_count(dataset)
        except Exception as e:
            nested.rollback()
            logger.error("Error upserting dataset '%s': %s", slug, e)
            raise
        else:
            nested.commit()
            try:
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
            if dataset is None:
                dataset = self.get_dataset_by_slug(slug)
            if dataset is not None:
                self.db.refresh(dataset)
            return dataset

    def delete_dataset_by_slug(self, slug: str) -> bool:
        """Delete a dataset by slug.

        Returns True if deleted, False if slug is falsy or dataset not found.
        On unexpected errors the transaction is rolled back and the exception is raised.
        """
        if not slug:
            return False

        try:
            dataset = self.get_dataset_by_slug(slug)
            if dataset is None:
                return False

            self.db.delete(dataset)
            self.db.commit()
            return True
        except Exception as e:
            logger.error("Error deleting dataset '%s': %s", slug, e)
            self.db.rollback()
            raise

    def get_dataset_by_slug(self, slug: str):
        if not slug:
            return None
        return self.db.query(Dataset).filter_by(slug=slug).first()

    def get_all_outdated_records(self, days=365, source_id=None):
        """
        gets all outdated versions of records older than [days] ago
        for all harvest sources. "outdated" simply means not the latest
        or the opposite of 'get_latest_harvest_records_by_source'
        """

        old_records_query = self.db.query(HarvestRecord).filter(
            func.extract("days", (func.now() - HarvestRecord.date_created)) > days
        )

        queries = [HarvestRecord.status == "success"]

        if source_id is not None:
            queries.append(HarvestRecord.harvest_source_id == source_id)

        subq = (
            self.db.query(HarvestRecord)
            .filter(*queries)
            .order_by(
                HarvestRecord.identifier,
                HarvestRecord.harvest_source_id,
                desc(HarvestRecord.date_created),
            )
            .distinct(HarvestRecord.identifier, HarvestRecord.harvest_source_id)
            .subquery()
        )

        sq_alias = aliased(HarvestRecord, subq)
        latest_successful_records_query = self.db.query(sq_alias).filter(
            sq_alias.action != "delete"
        )

        return old_records_query.except_all(latest_successful_records_query).all()

    @count_wrapper
    @count
    def get_latest_harvest_records_by_source_orm(
        self, source_id, synced=False, **kwargs
    ):
        """Get latest records for each harvest source

        :param string source_id - id for harvest source
        :param bool synced - only return records with ckan_id

        """
        # datetimes are returned as datetime objs not strs
        queries = [
            HarvestRecord.status == "success",
            HarvestRecord.harvest_source_id == source_id,
        ]
        if synced:
            queries.append(HarvestRecord.ckan_id.isnot(None))

        subq = (
            self.db.query(HarvestRecord)
            .filter(*queries)
            .order_by(HarvestRecord.identifier, desc(HarvestRecord.date_created))
            .distinct(HarvestRecord.identifier)
            .subquery()
        )
        sq_alias = aliased(HarvestRecord, subq)

        return (
            self.db.query(
                sq_alias.identifier,
                sq_alias.source_hash,
                sq_alias.ckan_id,
                sq_alias.date_created,
                sq_alias.date_finished,
                sq_alias.id,
                sq_alias.action,
                Dataset.slug.label("dataset_slug"),
            )
            .outerjoin(Dataset, Dataset.harvest_record_id == sq_alias.id)
            .filter(sq_alias.action != "delete")
        )

    def get_latest_harvest_records_by_source(self, source_id):
        return [
            dict(row._mapping)
            for row in self.get_latest_harvest_records_by_source_orm(source_id)
        ]

    def get_geo_from_string(self, location_name):
        """get a geometry from the locations table using the location name
        (e.g. California, New York)"""
        try:
            location = (
                self.db.query(func.ST_AsGeoJSON(Locations.the_geom))
                .filter(Locations.display_name.ilike(f"%{location_name}%"))
                .scalar()
            )
            return location
        except Exception as e:
            logger.error(
                'Error querying "{}" locations table {}'.format(location_name, e)
            )
            return None

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
            logger.error("Error: %s", e)
            self.db.rollback()
            return False, "An error occurred while adding the user."

    def list_users(self):
        try:
            return self.db.query(HarvestUser).all()
        except Exception as e:
            logger.error("Error: %s", e)
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
            logger.error("Error: %s", e)
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
            logger.error("Error: %s", e)
            return False

    #### PAGINATED QUERIES ####
    @count
    @paginate
    def pget_db_query(self, model=None, facets="", order_by="asc", **kwargs):
        model_map = {
            "organizations": Organization,
            "harvest_sources": HarvestSource,
            "harvest_records": HarvestRecord,
            "harvest_jobs": HarvestJob,
            "harvest_job_errors": HarvestJobError,
            "harvest_record_errors": HarvestRecordError,
        }
        model_class = model_map.get(model)
        if model_class is None:
            return f"Incorrect model arg {model}", 400

        facet_list = query_filter_builder(model_class, facets)

        # TODO: should we add date_created to these models??
        if model in ["organizations", "harvest_sources"]:
            return self.db.query(model_class).filter(*facet_list)

        order_by_val = order_by_helper(model_class, order_by)

        return self.db.query(model_class).filter(*facet_list).order_by(order_by_val)

    #### FILTERED BUILDER QUERIES ####
    def pget_organizations(self, facets="", **kwargs):
        return self.pget_db_query(model="organizations", facets=facets, **kwargs)

    def pget_harvest_sources(self, facets="", **kwargs):
        return self.pget_db_query(model="harvest_sources", facets=facets, **kwargs)

    def pget_harvest_records(self, facets="", order_by="asc", **kwargs):
        return self.pget_db_query(
            model="harvest_records", facets=facets, order_by=order_by, **kwargs
        )

    def pget_harvest_jobs(self, facets="", order_by="asc", **kwargs):
        return self.pget_db_query(
            model="harvest_jobs", facets=facets, order_by=order_by, **kwargs
        )

    def pget_harvest_job_errors(self, facets="", order_by="asc", **kwargs):
        return self.pget_db_query(
            model="harvest_job_errors", facets=facets, order_by=order_by, **kwargs
        )

    def pget_harvest_record_errors(self, facets="", order_by="asc", **kwargs):
        return self.pget_db_query(
            model="harvest_record_errors", facets=facets, order_by=order_by, **kwargs
        )

    def get_harvest_records_by_job(self, job_id, facets="", order_by="asc", **kwargs):
        facet_string = f"harvest_job_id eq {job_id}"
        if facets:
            facet_string += "," + facets
        return self.pget_db_query(
            model="harvest_records", facets=facet_string, order_by=order_by, **kwargs
        )

    def get_harvest_records_by_source(
        self, source_id, facets="", order_by="asc", **kwargs
    ):
        facet_string = f"harvest_source_id eq {source_id}"
        if facets:
            facet_string += "," + facets
        return self.pget_db_query(
            model="harvest_records", facets=facet_string, order_by=order_by, **kwargs
        )

    def refresh_dataset_mv(self):
        try:
            self.db.execute(
                text(
                    """
                    DO $$
                    BEGIN
                        IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'dataset' AND relkind = 'm') THEN
                            REFRESH MATERIALIZED VIEW CONCURRENTLY dataset;
                        END IF;
                    END $$;
                """
                )
            )
            self.db.commit()
        except Exception as e:
            logger.error("Error: %s", e)
            self.db.rollback()

    def insert_view_counts_of_datasets(self, datasets):
        """
        truncates then populates dataset view count table

        used by scripts/update_dataset_view_count.py in monthly github action. data
        comes from google analytics.
        """

        try:
            self.db.execute(text("TRUNCATE TABLE dataset_view_count"))

            # postgres caps query parameters to signed 2**16
            # going with half (roughly 5000*3)
            size = 5000
            while datasets:
                self.db.execute(insert(DatasetViewCount).values(datasets[:size]))
                datasets = datasets[size:]

            self.db.commit()
        except Exception as e:
            logger.error("Error: %s", e)
            self.db.rollback()


def order_by_helper(model, order_by):
    return model.date_created.asc() if order_by == "asc" else model.date_created.desc()
