from datetime import datetime

import click
from flask import Blueprint

from database.interface import HarvesterDBInterface
from shared.constants import SCHEMA_TYPE_VALUES

from .evaluate_sources import evaluate_sources

source = Blueprint("harvest_source", __name__)

db = HarvesterDBInterface()


@source.cli.command("list")
def cli_list_harvest_source():
    """List all harvest sources"""
    harvest_sources = db.get_all_harvest_sources()
    if harvest_sources:
        for source in harvest_sources:
            print(f"{source.name} : {source.id}")
    else:
        print("No harvest sources found.")


@source.cli.command("delete")
@click.argument("id")
def cli_remove_harvest_source(id):
    """Remove a harvest source with a given id."""
    message, status = db.delete_harvest_source(id)
    print(message)
    if status != 200:
        raise SystemExit(1)


@source.cli.command("evaluate_sources")
def cli_evaluate_sources():
    """
    Evaluates existing sources to see if they are still availible,
    captures the response code, and schema type.
    """
    evaluate_sources()


@source.cli.command("force_reharvest_sources")
@click.option(
    "--schema-type-prefix",
    "schema_type_prefixes",
    multiple=True,
    required=True,
    help=(
        "Force-reharvest sources whose schema_type starts with this prefix. "
        "Repeatable, matched with startswith() against the source's "
        f"schema_type (one of: {', '.join(SCHEMA_TYPE_VALUES)}). "
        "E.g. 'dcatus' matches all DCAT-US schema types, 'iso19115' matches "
        "both ISO schema types."
    ),
)
@click.option(
    "--dry-run/--no-dry-run",
    default=True,
    type=bool,
    help="List matching harvest sources without queuing any jobs.",
)
def cli_force_reharvest_sources(schema_type_prefixes, dry_run):
    """
    Force-reharvest every harvest source whose schema_type starts with one of
    the given --schema-type-prefix values. Dry run mode is enabled by default
    to prevent accidental mass triggering.

    Queues a "new" force_harvest job per source rather than starting each
    job's task immediately; the app's own scheduler picks up "new" jobs and
    starts them under its existing HARVEST_RUNNER_MAX_TASKS cap, the same way
    it drains regularly-scheduled harvests.

    Run against a deployed environment with, e.g.:
    `cf run-task datagov-harvest --command "flask harvest_source
    force_reharvest_sources --schema-type-prefix dcatus --no-dry-run"`
    """
    sources = [
        s
        for s in db.get_all_harvest_sources()
        if s.schema_type.startswith(schema_type_prefixes)
    ]
    prefixes_label = ", ".join(schema_type_prefixes)
    print(f"Found {len(sources)} harvest source(s) matching '{prefixes_label}'.")

    if dry_run:
        for s in sources:
            print(f"  {s.id}  {s.name}  ({s.schema_type})")
        print("Dry run: no jobs queued. Use --no-dry-run to queue them.")
        return

    for s in sources:
        active_job = db.get_active_harvest_job_for_source(s.id)
        if active_job:
            print(
                f"{s.id} ({s.name}): skipped, "
                f"job {active_job.id} already new/in_progress"
            )
            continue

        job = db.add_harvest_job(
            {
                "harvest_source_id": s.id,
                "status": "new",
                "job_type": "force_harvest",
                "date_created": datetime.now(),
            }
        )
        if job:
            print(f"{s.id} ({s.name}): queued job {job.id}")
        else:
            print(f"{s.id} ({s.name}): failed to queue job")
