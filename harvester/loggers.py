import logging


class LogDBHandler(logging.Handler):
    def __init__(self, session):
        logging.Handler.__init__(self)
        self.session = session

    def emit(self, record):
        print(record)

        # TODO: wire up storing the harvest_error records somewhere
        # harvest_error = HarvestError(
        #     message=record.__dict__["msg"],
        #     harvest_job_id=record.__dict__["harvest_job_id"],
        #     severity=record.__dict__["severity"],
        #     type=record.__dict__["type"],
        #     date_created=datetime.now(),
        # )
        # self.session.add(harvest_error)
        # self.session.commit()
