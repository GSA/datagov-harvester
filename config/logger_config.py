LOGGING_CONFIG = {
    "version": 1,
    "formatters": {
        "standard": {
            "format": (
                "[%(asctime)s] %(levelname)s "
                "[%(name)s.%(funcName)s:%(lineno)d] %(message)s"
            )
        },
    },
    "handlers": {
        "console": {
            "level": "INFO",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "harvest_runner": {
            "handlers": ["console"],
            "level": "INFO",
        },
    },
}
