from flask import Blueprint

main = Blueprint("main", __name__)

from . import auth, harvest_jobs, harvest_sources, organizations, pages  # noqa: E402, F401
