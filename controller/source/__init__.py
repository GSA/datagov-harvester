from flask import Blueprint

bp = Blueprint('source', __name__)
db = {}

from controller.source import create, view

