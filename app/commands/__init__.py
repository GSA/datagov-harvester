from .database import database
from .job import job
from .org import org
from .search import search
from .source import source
from .testdata import testdata
from .user import user


def register_commands(app):
    app.register_blueprint(database)
    app.register_blueprint(testdata)
    app.register_blueprint(user)
    app.register_blueprint(org)
    app.register_blueprint(source)
    app.register_blueprint(job)
    app.register_blueprint(search)
