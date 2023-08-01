from flask import Blueprint
from controller.dataset import create, view

bp = Blueprint('dataset', __name__)
db = {}