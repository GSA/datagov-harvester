# ruff: noqa: F401
from datagov_data_access.db.interfaces.harvest import (
    PAGINATE_ENTRIES_PER_PAGE,
    PAGINATE_START_PAGE,
)
from datagov_data_access.db.interfaces.harvest import (
    HarvesterDBInterface as db_interface,
)

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
