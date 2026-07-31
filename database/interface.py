# ruff: noqa: F401
from datagov_data_access.db.interfaces.harvest import (
    PAGINATE_ENTRIES_PER_PAGE,
    PAGINATE_START_PAGE,
)
from datagov_data_access.db.interfaces.harvest import (
    HarvesterDBInterface as db_interface,
)
from sqlalchemy import func, text

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


# wrap the data_access interface and uses the flask-sqlalchemy session as default.
# the data_access "HarvesterDBInterface" expects a session object because
# it no longer exists alongside the flask app.
class HarvesterDBInterface(db_interface):
    def __init__(self, session=None, *args, **kwargs):
        db_session = session if session else db.session
        super().__init__(db_session, *args, **kwargs)

    def get_harvest_record_errors_by_job(self, job_id: str, severity=None, **kwargs):
        return super().get_harvest_record_errors_by_job(
            job_id,
            severity=severity,
            **kwargs,
        )

    def get_harvest_record_errors_by_job_for_view(
        self, job_id: str, severity=None, **kwargs
    ):
        error_types = ["ValidationException", "ValidationError"]
        base = self.db.query(
            HarvestRecordError.harvest_record_id.label("harvest_record_id"),
            HarvestRecordError.harvest_job_id.label("harvest_job_id"),
            HarvestRecordError.date_created.label("date_created"),
            HarvestRecordError.type.label("type"),
            HarvestRecordError.message.label("message"),
            HarvestRecordError.severity.label("severity"),
            HarvestRecordError.id.label("id"),
        ).filter(HarvestRecordError.harvest_job_id == job_id)
        if severity is not None:
            base = base.filter(HarvestRecordError.severity == severity)
        base = base.subquery()

        instance_idx = 1
        agg = (
            self.db.query(
                func.array_agg(base.c.harvest_record_id)[instance_idx].label(
                    "harvest_record_id"
                ),
                func.array_agg(base.c.harvest_job_id)[instance_idx].label(
                    "harvest_job_id"
                ),
                func.array_agg(base.c.date_created)[instance_idx].label("date_created"),
                func.array_agg(base.c.type)[instance_idx].label("type"),
                func.array_to_string(func.array_agg(base.c.message), "::").label(
                    "message"
                ),
                func.array_agg(base.c.severity)[instance_idx].label("severity"),
                func.array_agg(base.c.id)[instance_idx].label("id"),
            )
            .filter(base.c.type.in_(error_types))
            .group_by(base.c.harvest_record_id)
        )

        other = self.db.query(
            base.c.harvest_record_id,
            base.c.harvest_job_id,
            base.c.date_created,
            base.c.type,
            base.c.message,
            base.c.severity,
            base.c.id,
        ).filter(base.c.type.not_in(error_types))

        grouped = agg.union_all(other).subquery()
        if kwargs.get("count") is True:
            return self.db.query(grouped.c.harvest_record_id)

        per_page = kwargs.get("per_page") or PAGINATE_ENTRIES_PER_PAGE
        page = kwargs.get("page") or PAGINATE_START_PAGE
        paged = grouped.select().limit(per_page).offset(page * per_page).subquery()

        return (
            self.db.query(
                paged.c.harvest_record_id,
                paged.c.harvest_job_id,
                paged.c.date_created,
                paged.c.type,
                paged.c.message,
                paged.c.severity,
                paged.c.id,
                HarvestRecord.identifier,
                HarvestRecord.source_raw,
            )
            .outerjoin(HarvestRecord, HarvestRecord.id == paged.c.harvest_record_id)
            .all()
        )

    def get_record_errors_summary_by_job(self, job_id: str):
        query = (
            self.db.query(
                HarvestRecordError.severity,
                HarvestRecordError.type,
                func.count(),
            )
            .where(HarvestRecordError.harvest_job_id == job_id)
            .group_by(HarvestRecordError.severity, HarvestRecordError.type)
            .order_by(HarvestRecordError.severity, HarvestRecordError.type)
        )
        return [
            {"severity": severity, "type": error_type, "count": error_count}
            for severity, error_type, error_count in query
        ]

    def stream_harvest_record_errors_by_job(self, job_id: str, batch_size=1000):
        query = text("""
            SELECT
                harvest_record_error.id,
                harvest_record.identifier,
                CASE
                    WHEN left(ltrim(harvest_record.source_raw), 1) IN ('{', '[')
                    THEN harvest_record.source_raw
                    ELSE NULL
                END AS source_raw,
                harvest_record_error.harvest_record_id,
                harvest_record_error.type,
                harvest_record_error.severity,
                harvest_record_error.message,
                harvest_record_error.date_created
            FROM harvest_record_error
            LEFT OUTER JOIN harvest_record
                ON harvest_record.id = harvest_record_error.harvest_record_id
            WHERE harvest_record_error.harvest_job_id = :job_id
        """)
        return (
            self.db.execute(query, {"job_id": job_id}).mappings().yield_per(batch_size)
        )
