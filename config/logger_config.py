LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
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
        "harvest_admin": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False,
        },
        "harvest_runner": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False,
        },
    },
}
