import click
from flask import Blueprint

from database.interface import HarvesterDBInterface

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
