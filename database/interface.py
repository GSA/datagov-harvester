# ruff: noqa: F401
from datagov_data_access.db.interfaces.harvest import (
    PAGINATE_ENTRIES_PER_PAGE,
    PAGINATE_START_PAGE,
)
from datagov_data_access.db.interfaces.harvest import (
    HarvesterDBInterface as db_interface,
)

from harvester.runner_settings import harvest_scheduling_is_disabled

from .models import db


# Wrap the data-access interface and use the Flask-SQLAlchemy session by default.
# The data-access HarvesterDBInterface expects a session because it no longer
# exists alongside the Flask app.
class HarvesterDBInterface(db_interface):
    def __init__(self, session=None, *args, **kwargs):
        db_session = session if session else db.session
        super().__init__(db_session, *args, **kwargs)

    def update_dataset_slug(self, dataset_id: str, new_slug: str):
        if not dataset_id or not new_slug:
            raise ValueError("dataset_id and new_slug are required")
        if harvest_scheduling_is_disabled():
            return (
                None,
                False,
                "Dataset updates are paused during OpenSearch maintenance.",
            )
        return super().update_dataset_slug(dataset_id, new_slug)
