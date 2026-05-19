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

    # `DROP SCHEMA ... CASCADE` can block on other open sessions. Since this
    # command is intentionally destructive, terminate other sessions first.
    click.echo("Terminating other database sessions...")
    db.session.execute(text("""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
              AND datname = current_database()
            """))

    # Rebuild from an empty schema so Alembic remains the source of truth.
    click.echo("Dropping and recreating schema...")
    db.session.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
    db.session.execute(text("CREATE SCHEMA public"))
    db.session.execute(text("GRANT ALL ON SCHEMA public TO public"))
    db.session.commit()

    click.echo("Applying migrations...")
    upgrade()
    click.echo("Database reset complete.")
