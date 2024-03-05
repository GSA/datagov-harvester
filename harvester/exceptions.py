import logging

# logging.getLogger(name) follows a singleton pattern
logger = logging.getLogger("harvest_runner")


# critical exceptions
class HarvestCriticalException(Exception):
    def __init__(self, msg, harvest_job_id):
        super().__init__(msg, harvest_job_id)

        self.msg = msg
        self.harvest_job_id = harvest_job_id
        self.severity = "CRITICAL"
        self.type = "job"

        self.extras = {
            # m = message. need to avoid conflicting with existing kwargs
            "m": self.msg,
            "harvest_job_id": self.harvest_job_id,
            "severity": self.severity,
            "type": self.type,
        }

        logger.critical(self.msg, exc_info=True, extra=self.extras)


class ExtractHarvestSourceException(HarvestCriticalException):
    pass


class ExtractCKANSourceException(HarvestCriticalException):
    pass


class CompareException(HarvestCriticalException):
    pass


# non-critical exceptions
class HarvestNonCriticalException(Exception):
    def __init__(self, msg, harvest_job_id):
        super().__init__(msg, harvest_job_id)

        self.msg = msg
        self.harvest_job_id = harvest_job_id
        self.severity = "ERROR"
        self.type = "record"

        extras = {
            # m = message. need to avoid conflicting with existing kwargs
            "m": self.msg,
            "harvest_job_id": self.harvest_job_id,
            "severity": self.severity,
            "type": self.type,
        }

        logger.error(msg, exc_info=True, extra=extras)


class ValidationException(HarvestNonCriticalException):
    pass


class TranformationException(HarvestNonCriticalException):
    pass


class DCATUSToCKANException(HarvestNonCriticalException):
    pass


class SynchronizeException(HarvestNonCriticalException):
    pass
