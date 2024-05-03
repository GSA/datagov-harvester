import logging
from datetime import datetime

from database.interface import HarvesterDBInterface


# critical exceptions
class HarvestCriticalException(Exception):
    def __init__(self, msg, harvest_job_id):
        super().__init__(msg, harvest_job_id)

        self.msg = msg
        self.harvest_job_id = harvest_job_id
        self.severity = "CRITICAL"
        self.type = "job"

        self.db_interface = HarvesterDBInterface()
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
    def __init__(self, msg, harvest_job_id, title):
        super().__init__(msg, harvest_job_id, title)

        self.title = title
        self.msg = msg
        self.harvest_job_id = harvest_job_id
        self.severity = "ERROR"
        self.type = "record"

        self.db_interface = HarvesterDBInterface()
        self.logger = logging.getLogger("harvest_runner")

        error_data = {
            "harvest_job_id": self.harvest_job_id,
            "message": self.msg,
            "severity": self.severity,
            "type": self.type,
            "date_created": datetime.utcnow(),
            "harvest_record_id": self.title,  # to-do
        }

        self.db_interface.add_harvest_error(error_data)
        self.logger.error(self.msg, exc_info=True)


class ValidationException(HarvestNonCriticalException):
    pass


class TranformationException(HarvestNonCriticalException):
    pass


class DCATUSToCKANException(HarvestNonCriticalException):
    pass


class SynchronizeException(HarvestNonCriticalException):
    pass
