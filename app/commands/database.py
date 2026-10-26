import click
from flask import Blueprint
from flask_migrate import upgrade
from sqlalchemy import text

from database.interface import HarvesterDBInterface
from database.models import db

database = Blueprint("database", __name__)
interface = HarvesterDBInterface()


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


@database.cli.command("backfill-current-record-issues")
@click.option(
    "--dry-run/--no-dry-run",
    default=True,
    type=bool,
    help="Report what would be resynced without writing anything.",
)
def backfill_current_record_issues(dry_run: bool) -> None:
    """One-time backfill of harvest_record_current_issue for warnings/errors
    that predate that table (#6440).

    harvest_record_current_issue is only kept in sync going forward, as a
    record gets reprocessed. An unchanged record is never reprocessed, so an
    identifier whose last real update predates #6440 stays missing from the
    table forever, even though other identifiers in the same source may
    already be synced from their own more recent runs. Checking per source
    rather than per identifier would let those already-synced identifiers
    mask the gap, so this resyncs every identifier's latest successful
    harvest_record unconditionally, the same way sync_current_record_issues
    does for a single record during a live harvest run.

    https://github.com/GSA/data.gov/issues/6471
    """
    touched_sources = 0
    resynced_identifiers = 0

    for source in interface.get_all_harvest_sources():
        latest_records = interface.get_latest_harvest_records_by_source_orm(source.id)
        if not latest_records:
            continue

        click.echo(
            f"{source.id} ({source.name}): resyncing {len(latest_records)} "
            "identifier(s)"
        )
        touched_sources += 1
        resynced_identifiers += len(latest_records)

        if dry_run:
            continue

        for record in latest_records:
            interface.sync_current_record_issues(
                source.id, record.record_type, record.identifier, record.id
            )

    click.echo(
        f"{'Would resync' if dry_run else 'Resynced'} {resynced_identifiers} "
        f"identifier(s) across {touched_sources} harvest source(s)."
    )
    if dry_run:
        click.echo("Use --no-dry-run to write the changes.")
