import click
from flask import Blueprint
from flask_migrate import upgrade
from sqlalchemy import text

from database.models import db

database = Blueprint("database", __name__)


@database.cli.command("reset")
@click.option(
    "--yes",
    is_flag=True,
    default=False,
    help="Confirm destructive reset of the database schema.",
)
def reset_database(yes: bool) -> None:
    """Drop the public schema and rebuild the database from migrations."""
    if not yes:
        click.confirm(
            "This will permanently delete all data in the database. Continue?",
            default=False,
            abort=True,
        )

    # Rebuild from an empty schema so Alembic remains the source of truth.
    db.session.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
    db.session.execute(text("CREATE SCHEMA public"))
    db.session.execute(text("GRANT ALL ON SCHEMA public TO public"))
    db.session.commit()

    upgrade()
    click.echo("Database reset complete.")
