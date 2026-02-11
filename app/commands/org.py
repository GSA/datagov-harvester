import click
from flask import Blueprint
from werkzeug.datastructures import MultiDict

from database.interface import HarvesterDBInterface

from ..forms import OrganizationForm
from ..util import make_new_org_contract

org = Blueprint("org", __name__)

db = HarvesterDBInterface()


@org.cli.command("add")
@click.argument("name")
@click.option("--slug", default="", help="Slug for the organization")
@click.option(
    "--logo",
    default="https://raw.githubusercontent.com/GSA/datagov-harvester/refs/heads/main/app/static/assets/img/placeholder-organization.png",
    help="Org Logo",
)
@click.option(
    "--aliases",
    default="",
    help="Comma-separated list of organization aliases",
)
@click.option("--id", help="Org ID: should correspond to CKAN ORG ID")
def cli_add_org(name, slug, logo, id, aliases):
    # let the web UI handle the validation, mostly for slug uniqueness
    form_data = MultiDict(
        {
            "name": name,
            "slug": slug,
            "logo": logo,
            "description": "",
            "organization_type": "",
            "aliases": aliases,
        }
    )

    form = OrganizationForm(formdata=form_data, meta={"csrf": False})
    if not form.validate():
        click.echo("Failed to add organization.")
        for field, errors in form.errors.items():
            for error in errors:
                click.echo(f" - {field}: {error}")
        return

    org_contract = make_new_org_contract(form)
    if id:
        org_contract["id"] = id

    org = db.add_organization(org_contract)
    if org:
        print(f"Added new organization with ID: {org.id}")
    else:
        print("Failed to add organization.")


@org.cli.command("list")
def cli_list_org():
    """List all organizations"""
    organizations = db.get_all_organizations()
    if organizations:
        for org in organizations:
            print(f"{org.name} : {org.id}")
    else:
        print("No organizations found.")


@org.cli.command("delete")
@click.argument("id")
def cli_remove_org(id):
    """Remove an organization with a given id."""
    result = db.delete_organization(id)
    if result:
        print(f"Triggered delete of organization with ID: {id}")
    else:
        print("Failed to delete organization")
