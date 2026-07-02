from flask import Blueprint

main = Blueprint("main", __name__)

from . import (  # noqa: E402, F401
    auth,
    harvest_jobs,
    harvest_sources,
    organizations,
    pages,
)
