# ruff: noqa: F401
from datagov_data_access.db.models import (
    Base,
    Dataset,
    DatasetViewCount,
    Error,
    HarvestJob,
    HarvestJobError,
    HarvestRecord,
    HarvestRecordError,
    HarvestSource,
    HarvestUser,
    Locations,
    Organization,
    ResourceViewCount,
)
from datagov_data_access.shared.constants import (
    FREQUENCY_VALUES,
    JOB_STATUS_VALUES,
    NOTIFICATION_FREQUENCY_VALUES,
    ORGANIZATION_TYPE_VALUES,
    SCHEMA_TYPE_VALUES,
    SEVERITY_VALUES,
    SOURCE_TYPE_VALUES,
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Column, Integer, text

# HarvestJob is imported from datagov-data-access. patch in this local
# column until that dependency has a new models.py released.
# this way I can run the migration and push to dev without having to wait.
if "records_warned" not in HarvestJob.__table__.c:
    HarvestJob.records_warned = Column(
        "records_warned",
        Integer,
        nullable=True,
        server_default=text("0"),
    )


def _harvest_job_to_dict(self):
    column_names = [c.name for c in self.__table__.columns]
    if "records_warned" in column_names:
        column_names.remove("records_warned")
        errored_index = column_names.index("records_errored")
        column_names.insert(errored_index + 1, "records_warned")

    return {name: getattr(self, name) for name in column_names}


HarvestJob.to_dict = _harvest_job_to_dict

db = SQLAlchemy(model_class=Base)
