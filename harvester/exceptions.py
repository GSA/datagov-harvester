import logging
from datetime import datetime

from . import db_interface


# critical exceptions
class HarvestCriticalException(Exception):
    def __init__(self, msg, harvest_job_id):
        super().__init__(msg, harvest_job_id)

        self.msg = msg
        self.harvest_job_id = harvest_job_id
        self.severity = "CRITICAL"
        self.type = "job"

        self.db_interface = db_interface
        self.logger = logging.getLogger("harvest_runner")

        error_data = {
            "harvest_job_id": self.harvest_job_id,
            "message": self.msg,
            "severity": self.severity,
            "type": self.type,
            "date_created": datetime.utcnow(),
        }

        self.db_interface.add_harvest_error(error_data)
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
        self.severity = "ERROR"
        self.type = "record"
        self.harvest_record_id = record_id

        self.db_interface = db_interface
        self.logger = logging.getLogger("harvest_runner")

        error_data = {
            "harvest_job_id": self.harvest_job_id,
            "message": self.msg,
            "severity": self.severity,
            "type": self.type,
            "date_created": datetime.utcnow(),
            "harvest_record_id": record_id,  # to-do
        }

        self.db_interface.add_harvest_error(error_data)
        self.logger.error(self.msg, exc_info=True)


class ValidationException(HarvestNonCriticalException):
    def __init__(self, msg, harvest_job_id, record_id):
        super().__init__(msg, harvest_job_id, record_id)

        self.db_interface.update_harvest_record(record_id, {"status": "error"})


class TranformationException(HarvestNonCriticalException):
    pass


class DCATUSToCKANException(HarvestNonCriticalException):
    pass


class SynchronizeException(HarvestNonCriticalException):
    pass
