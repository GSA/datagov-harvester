from flask import Blueprint

bp = Blueprint('job', __name__)
db = {}

from controller.job import run, status
