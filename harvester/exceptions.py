import logging
from datetime import datetime, timezone

from . import HarvesterDBInterface, db_interface


# critical exceptions
class HarvestCriticalException(Exception):
    def __init__(self, msg, harvest_job_id):
        super().__init__(msg, harvest_job_id)

        self.msg = msg
        self.harvest_job_id = harvest_job_id

        self.db_interface: HarvesterDBInterface = db_interface
        self.logger = logging.getLogger("harvest_runner")

        error_data = {
            "harvest_job_id": self.harvest_job_id,
            "message": self.msg,
            "type": self.__class__.__name__,
            "date_created": datetime.now(timezone.utc),
        }

        self.db_interface.add_harvest_error(error_data, "job")
        self.logger.critical(self.msg, exc_info=True)


class ExtractExternalException(HarvestCriticalException):
    pass


class ExtractInternalException(HarvestCriticalException):
    pass


class CompareException(HarvestCriticalException):
    pass


# non-critical exceptions
class HarvestNonCriticalException(Exception):
    def __init__(self, msg, harvest_job_id, record_id):
        super().__init__(msg, harvest_job_id, record_id)

        self.msg = msg
        self.harvest_job_id = harvest_job_id
        self.harvest_record_id = record_id

        self.db_interface: HarvesterDBInterface = db_interface
        self.logger = logging.getLogger("harvest_runner")

        error_data = {
            "message": self.msg,
            "type": self.__class__.__name__,
            "date_created": datetime.now(timezone.utc),
            "harvest_record_id": record_id,  # to-do
        }

        self.db_interface.add_harvest_error(error_data, "record")
        self.db_interface.update_harvest_record(record_id, {"status": "error"})
        self.logger.error(self.msg, exc_info=True)


class ValidationException(HarvestNonCriticalException):
    pass


class TranformationException(HarvestNonCriticalException):
    pass


class DCATUSToCKANException(HarvestNonCriticalException):
    pass


class SynchronizeException(HarvestNonCriticalException):
    pass
