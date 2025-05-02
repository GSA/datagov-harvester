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

        job_status = {
            "status": "error",
            "date_finished": datetime.now(timezone.utc),
        }

        self.db_interface.add_harvest_job_error(error_data)
        self.db_interface.update_harvest_job(harvest_job_id, job_status)
        self.log_err()

    def log_err(self):
        self.logger.critical(self.msg, exc_info=True)


class ExtractExternalException(HarvestCriticalException):
    pass


class ExtractInternalException(HarvestCriticalException):
    pass


class CompareException(HarvestCriticalException):
    pass


# non-critical exceptions
class HarvestNonCriticalException(Exception):
    def __init__(self, msg, harvest_job_id, harvest_record_id):
        super().__init__(msg, harvest_job_id, harvest_record_id)

        self.msg = msg
        self.harvest_job_id = harvest_job_id
        self.harvest_record_id = harvest_record_id

        self.db_interface: HarvesterDBInterface = db_interface
        self.logger = logging.getLogger("harvest_runner")

        error_data = {
            "message": self.msg,
            "type": self.__class__.__name__,
            "date_created": datetime.now(timezone.utc),
            "harvest_record_id": harvest_record_id,  # to-do
            "harvest_job_id": harvest_job_id,
        }

        self.db_interface.add_harvest_record_error(error_data)
        self.db_interface.update_harvest_record(harvest_record_id, {"status": "error"})
        self.log_err()

    def log_err(self):
        self.logger.error(self.msg, exc_info=True)


class ValidationException(HarvestNonCriticalException):
    def log_err(self):
        pass

class DuplicateIdentifierException(HarvestNonCriticalException):
    pass

class TransformationException(HarvestNonCriticalException):
    pass


class DCATUSToCKANException(HarvestNonCriticalException):
    pass


class SynchronizeException(HarvestNonCriticalException):
    pass
