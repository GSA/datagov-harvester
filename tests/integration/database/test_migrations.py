import pytest
from alembic.script import ScriptDirectory
from flask import current_app
from flask_migrate import downgrade, upgrade

from database.models import db


@pytest.fixture(autouse=True)
def dbapp(app):
    """Override the root conftest's dbapp fixture.

    Migrations manage their own schema from an empty database, so this
    drops all ORM-created tables instead of also recreating them.
    """
    with app.app_context():
        db.drop_all()
    yield
    with app.app_context():
        db.drop_all()
        db.create_all()


def _ordered_revisions(app):
    with app.app_context():
        config = current_app.extensions["migrate"].migrate.get_config()
        script = ScriptDirectory.from_config(config)
        revisions = list(script.walk_revisions("base", "heads"))
        revisions.reverse()
        return revisions


def test_migrations_upgrade_and_downgrade_cleanly(app):
    revisions = _ordered_revisions(app)
    assert len(revisions) > 0

    with app.app_context():
        for revision in revisions:
            upgrade(revision=revision.revision)

            down = revision.down_revision or "base"
            downgrade(revision=down)
            upgrade(revision=revision.revision)
