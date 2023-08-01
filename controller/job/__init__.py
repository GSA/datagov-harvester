from flask import Blueprint
from controller.job import run, status

bp = Blueprint('job', __name__)
db = {}


