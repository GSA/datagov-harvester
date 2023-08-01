from flask import Blueprint

bp = Blueprint('dataset', __name__)
db = {}

from controller.dataset import create, view