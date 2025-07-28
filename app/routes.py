import csv
import json
import logging
import os
import secrets
import tempfile
import time
import uuid
from datetime import timedelta
from functools import wraps

import click
import jwt
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from dotenv import load_dotenv
from flask import (
    Blueprint,
    Response,
    flash,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from jinja2_fragments.flask import render_block
from markupsafe import escape

from database.interface import HarvesterDBInterface
from harvester.lib.load_manager import LoadManager
from harvester.utils.general_utils import (
    convert_to_int,
    dynamic_map_list_items_to_dict,
    get_datetime,
    is_it_true,
    process_job_complete_percentage,
)

from . import htmx
from .commands.evaluate_sources import evaluate_sources
from .forms import (
    HarvestSourceForm,
    HarvestTriggerForm,
    OrganizationForm,
    OrganizationTriggerForm,
)
from .paginate import Pagination

logger = logging.getLogger("harvest_admin")

user = Blueprint("user", __name__)
auth = Blueprint("auth", __name__)
main = Blueprint("main", __name__)
org = Blueprint("org", __name__)
source = Blueprint("harvest_source", __name__)
job = Blueprint("harvest_job", __name__)
api = Blueprint("api", __name__)
testdata = Blueprint("testdata", __name__)

db = HarvesterDBInterface()

load_manager = LoadManager()

# Login authentication
load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
REDIRECT_URI = os.getenv("REDIRECT_URI")
ISSUER = os.getenv("ISSUER")
AUTH_URL = ISSUER + "/openid_connect/authorize"
TOKEN_URL = ISSUER + "/api/openid_connect/token"

STATUS_STRINGS_ENUM = {"404": "Not Found"}


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        provided_token = request.headers.get("Authorization")
        if request.is_json or provided_token is not None:
            if provided_token is None:
                return "error: Authorization header missing", 401
            api_token = os.getenv("FLASK_APP_SECRET_KEY")
            if provided_token != api_token:
                return "error: Unauthorized", 401
            return f(*args, **kwargs)

        # check session-based authentication for web users
        if "user" not in session:
            session["next"] = request.url
            return redirect(url_for("main.login"))
        return f(*args, **kwargs)

    return decorated_function


def create_client_assertion():
    private_key_data = os.getenv("OPENID_PRIVATE_KEY")
    if not private_key_data:
        raise ValueError("No private key found in the environment variable")

    private_key = load_pem_private_key(
        private_key_data.encode("utf-8"), password=None, backend=default_backend()
    )

    now = int(time.time())
    payload = {
        "iss": CLIENT_ID,
        "sub": CLIENT_ID,
        "aud": TOKEN_URL,
        "jti": uuid.uuid4().hex,
        "exp": now + 900,  # Token is valid for 15 minutes
        "iat": now - 100,  # Issued at time, allow some leeway
    }

    return jwt.encode(payload, private_key, algorithm="RS256")


# HELPERS
# TODO: when refactoring routes put them in their respective files
## HARVEST SOURCE HELPERS
def trigger_manual_job_helper(source_id, job_type="harvest"):
    message = load_manager.trigger_manual_job(source_id, job_type)
    flash(message)
    return redirect(url_for("main.view_harvest_source", source_id=source_id))


@main.route("/login")
def login():
    state = secrets.token_urlsafe(32)
    nonce = secrets.token_urlsafe(32)
    session["state"] = state
    session["nonce"] = nonce

    auth_request_url = (
        f"{AUTH_URL}?response_type=code"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&scope=openid email"
        f"&state={state}"
        f"&nonce={nonce}"
        f"&acr_values=http://idmanagement.gov/ns/assurance/loa/1"
    )
    return redirect(auth_request_url)


@main.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("main.index"))


@main.route("/callback")
def callback():
    code = request.args.get("code")
    state = request.args.get("state")

    if state != session.pop("state", None):
        return "State mismatch error", 400

    client_assertion = create_client_assertion()
    # ruff: noqa: E501
    token_payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
        "client_assertion": client_assertion,
    }

    response = requests.post(TOKEN_URL, data=token_payload)

    if response.status_code != 200:
        return "Failed to fetch access token", 400

    token_data = response.json()
    id_token = token_data.get("id_token")

    decoded_id_token = jwt.decode(id_token, options={"verify_signature": False})

    usr_email = decoded_id_token["email"].lower()

    usr_info = {"email": usr_email, "ssoid": decoded_id_token["sub"]}
    usr = db.verify_user(usr_info)

    if usr:
        session["user"] = usr_email
        next_url = session.pop("next", None)
        if next_url:
            return redirect(url_for(next_url))
        else:
            return redirect(url_for("main.index"))
    else:
        flash("Please request registration from the admin before proceeding.")
        return redirect(url_for("main.index"))


# CLI commands
## User management
@user.cli.command("add")
@click.argument("email")
@click.option("--name", default="", help="Name of the user")
def add_user(email, name):
    """add new user with .gov email."""

    email = email.lower()
    usr_data = {"email": email}
    if name:
        usr_data["name"] = name

    success, message = db.add_user(usr_data)
    if success:
        print("User added successfully!")
    else:
        print("Error:", message)


@user.cli.command("list")
def list_users():
    """List all users' emails."""
    users = db.list_users()
    if users:
        for user in users:
            print(user.email)
    else:
        print("No users found.")


@user.cli.command("remove")
@click.argument("email")
def remove_user(email):
    """Remove a user with the given EMAIL."""
    email = email.lower()
    if db.remove_user(email):
        print(f"Removed user with email: {email}")
    else:
        print("Failed to remove user or user not found.")


## Org management
@org.cli.command("add")
@click.argument("name")
@click.option(
    "--logo",
    default="https://raw.githubusercontent.com/GSA/datagov-harvester/refs/heads/main/app/static/assets/img/placeholder-organization.png",
    help="Org Logo",
)
@click.option("--id", help="Org ID: should correspond to CKAN ORG ID")
def cli_add_org(name, logo, id):
    org_contract = {"name": name}
    if logo:
        org_contract["logo"] = logo
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


## Harvest Source Management
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
    result = db.delete_harvest_source(id)
    if result:
        print(f"Triggered delete of harvest source with ID: {id}")
    else:
        print("Failed to delete harvest source")


@source.cli.command("evaluate_sources")
def cli_evaluate_sources():
    """
    Evaluates existing sources to see if they are still availible,
    captures the response code, and schema type.
    """
    evaluate_sources()


## Harvet Job Management
@job.cli.command("delete")
@click.argument("id")
def cli_remove_harvest_job(id):
    """Remove a harvest job with a given id."""
    result = db.delete_harvest_job(id)
    if result:
        print(f"Triggered delete of harvest job with ID: {id}")
    else:
        print("Failed to delete harvest job")


## Load Test Data
# TODO move this into its own file when you break up routes
@testdata.cli.command("load_test_data")
def fixtures():
    """Load database fixtures from JSON."""
    import json

    file = "./tests/fixtures.json"
    click.echo(f"Loading fixtures at `{file}`.")
    with open(file, "r") as file:
        fixture = json.load(file)
        for item in fixture["organization"]:
            db.add_organization(item)
        for item in fixture["source"]:
            db.add_harvest_source(item)
        for item in fixture["job"]:
            db.add_harvest_job(item)
        for item in fixture["record"]:
            db.add_harvest_record(item)
        for item in fixture["record_error"]:
            db.add_harvest_record_error(item)

    click.echo("Done.")


# Helper Functions
def make_new_source_contract(form):
    return {
        "organization_id": form.organization_id.data,
        "name": form.name.data,
        "url": form.url.data,
        "notification_emails": form.notification_emails.data,
        "frequency": form.frequency.data,
        "schema_type": form.schema_type.data,
        "source_type": form.source_type.data,
        "notification_frequency": form.notification_frequency.data,
    }


def make_new_org_contract(form):
    return {"name": form.name.data, "logo": form.logo.data}


# Routes
@main.route("/", methods=["GET"])
def index():
    return redirect(url_for("main.organization_list"))


## Organizations
### Add Org
@main.route("/organization/add", methods=["POST", "GET"])
@login_required
def add_organization():
    if request.is_json:
        org = db.add_organization(request.json)
        if org:
            return make_response(
                jsonify({"message": f"Added new organization with ID: {org.id}"}), 200
            )
        else:
            return make_response(jsonify({"error": "Failed to add organization."}), 400)
    else:
        form = OrganizationForm()
        if form.validate_on_submit():
            new_org = {
                "name": form.name.data,
                "logo": form.logo.data,
            }
            org = db.add_organization(new_org)
            if org:
                flash(f"Added new organization with ID: {org.id}")
            else:
                flash("Failed to add organization.")
            return redirect(url_for("main.organization_list"))
        elif form.errors:
            flash(form.errors)
            return redirect(url_for("main.add_organization"))
    return render_template(
        "edit_data.html",
        form=form,
        action="Add",
        data_type="Organization",
        button="Submit",
    )


@main.route("/organization_list/", methods=["GET"])
def organization_list():
    organizations = db.get_all_organizations()
    if request.args.get("type") and request.args.get("type") == "json":
        return db._to_dict(organizations)
    else:
        data = {"organizations": organizations}
        return render_template("view_org_list.html", data=data)


@main.route("/organization/<org_id>", methods=["GET", "POST"])
def view_organization(org_id: str):
    if request.method == "POST":
        form = OrganizationTriggerForm(request.form)
        if form.data["edit"]:
            return redirect(url_for("main.edit_organization", org_id=org_id))
        elif form.data["delete"]:
            try:
                message, status = db.delete_organization(org_id)
                flash(message)
                if status == 409:
                    return redirect(url_for("main.view_organization", org_id=org_id))
                else:
                    return redirect(url_for("main.organization_list"))
            except Exception as e:
                message = f"Failed to delete organization :: {repr(e)}"
                logger.error(message)
                flash(message)
                return redirect(url_for("main.view_organization", org_id=org_id))
        else:
            return redirect(url_for("main.view_organization", org_id=org_id))
    else:
        org = db.get_organization(org_id)
        if request.is_json:
            if org is None:
                # org_id wasn't found
                return make_response(
                    jsonify({"message": "Organization not found"}), 404
                )
            return jsonify(org.to_dict())
        form = OrganizationTriggerForm()
        sources = db.get_harvest_source_by_org(org_id)
        future_harvest_jobs = {}
        for source in sources:
            job = db.get_new_harvest_jobs_by_source_in_future(source.id)
            if len(job):
                future_harvest_jobs[source.id] = job[0].date_created
        harvest_jobs = {}
        for source in sources:
            job = db.get_first_harvest_job_by_filter(
                {"harvest_source_id": source.id, "status": "complete"}
            )
            if job:
                harvest_jobs[source.id] = job
        data = {
            "organization": org,
            "organization_dict": db._to_dict(org),
            "harvest_sources": sources,
            "harvest_jobs": harvest_jobs,
            "future_harvest_jobs": future_harvest_jobs,
        }
        return render_template(
            "view_org_data.html",
            data=data,
            form=form,
        ), (200 if org is not None else 404)


### Edit Org
@main.route("/organization/edit/<org_id>", methods=["GET", "POST"])
@login_required
def edit_organization(org_id):
    if request.is_json:
        org = db.update_organization(org_id, request.json)
        if org:
            return {"message": f"Updated org with ID: {org.id}"}, 200
        else:
            return {"error": "Failed to update organization."}, 400

    org = db._to_dict(db.get_organization(org_id))
    form = OrganizationForm(data=org)
    if form.validate_on_submit():
        new_org_data = make_new_org_contract(form)
        org = db.update_organization(org_id, new_org_data)
        if org:
            flash(f"Updated org with ID: {org.id}")
        else:
            flash("Failed to update organization.")
        return redirect(url_for("main.view_organization", org_id=org_id))
    elif form.errors:
        flash(form.errors)
        return redirect(url_for("main.edit_organization", org_id=org_id))

    return render_template(
        "edit_data.html",
        form=form,
        action="Edit",
        data_type="Organization",
        button="Update",
    )


### Delete Org
@api.route("/organization/<org_id>", methods=["DELETE"])
@login_required
def delete_organization(org_id):
    try:
        message, status = db.delete_organization(org_id)
        return make_response(jsonify({"message": message}), status)
    except Exception as e:
        message = f"Failed to delete organization :: {repr(e)}"
        logger.error(message)
        return make_response(jsonify({"message": message}), 500)


## Harvest Source
### Add Source
@main.route("/harvest_source/add", methods=["POST", "GET"])
@login_required
def add_harvest_source():
    if request.is_json:
        try:
            source = db.add_harvest_source(request.json)
            job_message = load_manager.schedule_first_job(source.id)
            if source and job_message:
                return make_response(
                    jsonify(
                        {
                            "message": f"Added new harvest source with ID: {source.id}. {job_message}"
                        }
                    ),
                    200,
                )
        except Exception as e:
            message = "Failed to add harvest source."
            logger.error(f"{message} :: {repr(e)}")
            return make_response(jsonify({"message": message}), 500)
    else:
        form = HarvestSourceForm()
        organizations = db.get_all_organizations()
        organization_choices = [
            (str(org.id), f"{org.name} - {org.id}") for org in organizations
        ]
        form.organization_id.choices = organization_choices
        if form.validate_on_submit():
            new_source = make_new_source_contract(form)
            source = db.add_harvest_source(new_source)
            job_message = load_manager.schedule_first_job(source.id)
            if source and job_message:
                flash(f"Added new harvest source with ID: {source.id}. {job_message}")
            else:
                flash("Failed to add harvest source.")
            return redirect(url_for("main.harvest_source_list"))
        elif form.errors:
            flash(form.errors)
            return redirect(url_for("main.add_harvest_source"))
    return render_template(
        "edit_data.html",
        form=form,
        action="Add",
        data_type="Harvest Source",
        button="Submit",
    )


@main.route("/harvest_source/<source_id>", methods=["GET", "POST"])
def view_harvest_source(source_id: str):
    htmx_vars = {
        "target_div": "#paginated__harvest-jobs",
        "endpoint_url": f"/harvest_source/{source_id}",
    }
    
    # Get filtering parameters
    type_filter = request.args.get('type_filter', '').strip()
    status_filter = request.args.get('status_filter', '').strip()
    
    # Get sorting parameters
    sort_by = request.args.get('sort', 'date_created')
    sort_order = request.args.get('order', 'desc')
    
    # Validate sort_order
    if sort_order not in ['asc', 'desc']:
        sort_order = 'desc'
    
    # Build facets for filtering
    harvest_jobs_facets = (
        f"harvest_source_id = '{source_id}'"
    )
    
    # Add filters to facets
    if type_filter:
        harvest_jobs_facets += f" AND harvest_job_type = '{type_filter}'"
    if status_filter:
        harvest_jobs_facets += f" AND status = '{status_filter}'"
    
    jobs_count = db.pget_harvest_jobs(
        facets=harvest_jobs_facets,
        count=True,
    )

    pagination = Pagination(
        count=jobs_count,
        current=request.args.get("page", 1, type=convert_to_int),
    )

    jobs = db.pget_harvest_jobs(
        facets=harvest_jobs_facets,
        page=pagination.db_current,
        order_by=sort_order,
        sort_by=sort_by,
    )

    if htmx:
        data = {
            "source": {"id": source_id},
            "jobs": jobs,
            "htmx_vars": htmx_vars,
            "type_filter": type_filter,
            "status_filter": status_filter,
            "sort_by": sort_by,
            "sort_order": sort_order,
        }
        return render_block(
            "view_source_data.html",
            "htmx_paginated",
            data=data,
            pagination=pagination.to_dict(),
        )
    elif request.method == "POST":
        form = HarvestTriggerForm(request.form)
        if form.data["edit"]:
            return redirect(url_for("main.edit_harvest_source", source_id=source_id))
        elif form.data["harvest"]:
            if form.data["force_check"]:
                return trigger_manual_job_helper(source_id, "force_harvest")
            else:
                return trigger_manual_job_helper(source_id)

        elif form.data["clear"]:
            return trigger_manual_job_helper(source_id, "clear")
        elif form.data["delete"]:
            try:
                message, status = db.delete_harvest_source(source_id)
                flash(message)
                if status == 409:
                    return redirect(
                        url_for("main.view_harvest_source", source_id=source_id)
                    )
                else:
                    return redirect(url_for("main.harvest_source_list"))
            except Exception as e:
                message = f"Failed to delete harvest source :: {repr(e)}"
                logger.error(message)
                flash(message)
                return redirect(
                    url_for("main.view_harvest_source", source_id=source_id)
                )
        else:
            return redirect(url_for("main.view_harvest_source", source_id=source_id))

    else:
        form = HarvestTriggerForm()
        records_count = db.get_latest_harvest_records_by_source_orm(
            source_id=source_id,
            count=True,
        )
        synced_records_count = db.get_latest_harvest_records_by_source_orm(
            source_id=source_id,
            count=True,
            synced=True,
        )
        summary_data = {
            "records_count": records_count,
            "synced_records_count": synced_records_count,
            "last_job_errors": None,
            "last_job_finished": None,
            "next_job_scheduled": None,
            "active_job_in_progress": False,
        }

        if jobs:
            last_job = jobs[0]
            if last_job.status == "in_progress":
                summary_data["active_job_in_progress"] = True

            last_job_error_count = db.get_harvest_record_errors_by_job(
                count=True,
                job_id=last_job.id,
            )
            summary_data["last_job_errors"] = last_job_error_count
            summary_data["last_job_finished"] = last_job.date_finished

        future_jobs = db.get_new_harvest_jobs_by_source_in_future(source_id)

        if future_jobs:
            summary_data["next_job_scheduled"] = future_jobs[0].date_created

        chart_data_values = dynamic_map_list_items_to_dict(
            db._to_dict(jobs[::-1]),  # reverse the list order for the chart
            [
                "date_finished",
                "records_added",
                "records_deleted",
                "records_errored",
                "records_ignored",
            ],
        )
        chart_data = {
            "labels": chart_data_values["date_finished"],
            "datasets": [
                {
                    "label": "Added",
                    "data": chart_data_values["records_added"],
                    "borderColor": "green",
                    "backgroundColor": "green",
                },
                {
                    "label": "Deleted",
                    "data": chart_data_values["records_deleted"],
                    "borderColor": "black",
                    "backgroundColor": "black",
                },
                {
                    "label": "Errored",
                    "data": chart_data_values["records_errored"],
                    "borderColor": "red",
                    "backgroundColor": "red",
                },
                {
                    "label": "Ignored",
                    "data": chart_data_values["records_ignored"],
                    "borderColor": "grey",
                    "backgroundColor": "grey",
                },
            ],
        }
        source = db.get_harvest_source(source_id)
        
        # Get available types and statuses for filters
        all_source_jobs = db.pget_harvest_jobs(
            facets=f"harvest_source_id = '{source_id}' AND date_created <= '{get_datetime()}'",
            page=1,
            order_by="desc",
            limit=1000  # Large enough to get all jobs for this source
        )
        
        available_types = sorted(list(set(job.harvest_job_type for job in all_source_jobs if job.harvest_job_type)))
        available_statuses = sorted(list(set(job.status for job in all_source_jobs if job.status)))
        
        data = {
            "source": source,
            "summary_data": summary_data,
            "jobs": jobs,
            "chart_data": chart_data,
            "htmx_vars": htmx_vars,
            "available_types": available_types,
            "available_statuses": available_statuses,
            "type_filter": type_filter,
            "status_filter": status_filter,
            "sort_by": sort_by,
            "sort_order": sort_order,
        }
        return render_template(
            "view_source_data.html",
            form=form,
            pagination=pagination.to_dict(),
            data=data,
        )


@main.route("/harvest_source_list/", methods=["GET"])
def harvest_source_list():
    sources = db.get_all_harvest_sources()
    data = {"harvest_sources": sources}
    return render_template("view_source_list.html", data=data)


### Edit Source
@main.route("/harvest_source/edit/<source_id>", methods=["GET", "POST"])
@login_required
def edit_harvest_source(source_id: str):
    if request.is_json:
        updated_source = db.update_harvest_source(source_id, request.json)
        job_message = load_manager.schedule_first_job(updated_source.id)

        if updated_source and job_message:
            return {
                "message": f"Updated source with ID: {updated_source.id}. {job_message}"
            }, 200
        else:
            return {"error": "Failed to update harvest source"}, 400

    if source_id:
        source = db.get_harvest_source(source_id)
        organizations = db.get_all_organizations()
        if source and organizations:
            organization_choices = [
                (str(org["id"]), f"{org['name']} - {org['id']}")
                for org in db._to_dict(organizations)
            ]
            source.notification_emails = ", ".join(source.notification_emails)
            form = HarvestSourceForm(data=db._to_dict(source))
            form.organization_id.choices = organization_choices
            if form.validate_on_submit():
                new_source_data = make_new_source_contract(form)
                source = db.update_harvest_source(source_id, new_source_data)
                job_message = load_manager.schedule_first_job(source.id)
                if source and job_message:
                    flash(f"Updated source with ID: {source.id}. {job_message}")
                else:
                    flash("Failed to update harvest source.")
                return redirect(
                    url_for("main.view_harvest_source", source_id=source.id)
                )
            elif form.errors:
                flash(form.errors)
                return redirect(
                    url_for("main.edit_harvest_source", source_id=source_id)
                )
            return render_template(
                "edit_data.html",
                form=form,
                action="Edit",
                data_type="Harvest Source",
                button="Update",
                source_id=source_id,
            )
        else:
            flash(f"No source with id: {source_id}")
            return redirect(url_for("main.harvest_source_list"))

    organization_id = request.args.get("organization_id")
    if organization_id:
        source = db.get_harvest_source_by_org(organization_id)
        if not source:
            return "No harvest sources found for this organization", 404
    else:
        source = db.get_all_harvest_sources()
    return db._to_dict(source)


# Delete Source
@api.route("/harvest_source/<source_id>", methods=["DELETE"])
@login_required
def delete_harvest_source(source_id):
    try:
        message, status = db.delete_harvest_source(source_id)
        return make_response(jsonify({"message": message}), status)
    except Exception as e:
        message = f"Failed to delete harvest source :: {repr(e)}"
        logger.error(message)
        return make_response(jsonify({"message": message}), 500)


### Trigger Harvest
@api.route("/harvest_source/harvest/<source_id>/<job_type>", methods=["GET"])
@login_required
def trigger_harvest_source(source_id, job_type):
    message = load_manager.trigger_manual_job(source_id, job_type)
    flash(message)
    return redirect(f"/harvest_source/{source_id}")


## Harvest Job
### Add Job
@main.route("/harvest_job/add", methods=["POST"])
@login_required
def add_harvest_job():
    if request.is_json:
        job = db.add_harvest_job(request.json)
        if job:
            return {"message": f"Added new harvest job with ID: {job.id}"}, 200
        else:
            return {"error": "Failed to add harvest job."}, 400
    else:
        return {"Please provide harvest job with json format."}


### Get Job
@main.route("/harvest_job/<job_id>", methods=["GET"])
def view_harvest_job(job_id=None):
    def _load_json_title(json_string):
        try:
            return json.loads(json_string).get("title", None)
        except Exception as e:
            logger.error(f"Error loading json source_raw: {repr(e)}")
            return None

    record_error_count = db.get_harvest_record_errors_by_job(
        job_id,
        count=True,
    )
    htmx_vars = {
        "target_div": "#error_results_pagination",
        "endpoint_url": f"/harvest_job/{job_id}",
    }

    pagination = Pagination(
        count=record_error_count,
        current=request.args.get("page", 1, type=convert_to_int),
    )
    record_errors = db.get_harvest_record_errors_by_job(
        job_id,
        page=pagination.db_current,
    )
    record_errors_dict = [
        {
            "error": db._to_dict(row.HarvestRecordError),
            "identifier": row.identifier,
            "title": _load_json_title(row.source_raw),
        }
        for row in record_errors
    ]
    if htmx:
        data = {
            "harvest_job_id": job_id,
            "record_errors": record_errors_dict,
            "htmx_vars": htmx_vars,
        }
        return render_block(
            "view_job_data.html",
            "record_errors_table",
            data=data,
            pagination=pagination.to_dict(),
        )
    else:
        job = db.get_harvest_job(job_id)
        if request.args.get("type") and request.args.get("type") == "json":
            return db._to_dict(job) if job else (STATUS_STRINGS_ENUM["404"], 404)
        else:
            data = {
                "job": job,
                "record_errors": record_errors_dict,
                "htmx_vars": htmx_vars,
            }
            if job and job.status == "in_progress":
                data["percent_complete"] = process_job_complete_percentage(
                    job.to_dict()
                )

            return render_template(
                "view_job_data.html", data=data, pagination=pagination.to_dict()
            )


### Update Job
@main.route("/harvest_job/<job_id>", methods=["PUT"])
@login_required
def update_harvest_job(job_id):
    result = db.update_harvest_job(job_id, request.json)
    return db._to_dict(result)


### Delete Job
@main.route("/harvest_job/<job_id>", methods=["DELETE"])
@login_required
def delete_harvest_job(job_id):
    result = db.delete_harvest_job(job_id)
    return escape(result)


@main.route("/harvest_job/cancel/<job_id>", methods=["GET", "POST"])
@login_required
def cancel_harvest_job(job_id):
    """Cancels a harvest job"""
    # Validate job_id (e.g., ensure it is alphanumeric or a valid UUID)
    if not job_id.isalnum():
        flash("Invalid job ID.")
        return redirect("/")
    message = load_manager.stop_job(job_id)
    flash(message)
    return redirect(f"/harvest_job/{job_id}")


### Download all errors for a given job
@main.route("/harvest_job/<job_id>/errors/<error_type>", methods=["GET"])
def download_harvest_errors_by_job(job_id, error_type):
    try:
        if error_type not in ["job", "record"]:
            return "Invalid error type. Must be 'job' or 'record'", 400

        # Create a temporary file to store the CSV data
        temp_file = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv")
        temp_file_path = temp_file.name

        try:
            csv_writer = csv.writer(temp_file)

            if error_type == "job":
                # Write header
                header = [
                    "harvest_job_id",
                    "date_created",
                    "job_error_type",
                    "message",
                    "harvest_job_error_id",
                ]
                csv_writer.writerow(header)

                # Write job errors
                errors = db.get_harvest_job_errors_by_job(job_id)
                for error_dict in errors:
                    row = [
                        str(error_dict.get("harvest_job_id", "")),
                        str(error_dict.get("date_created", "")),
                        str(error_dict.get("type", "")),
                        str(error_dict.get("message", "")),
                        str(error_dict.get("id", "")),
                    ]
                    csv_writer.writerow(row)

            elif error_type == "record":
                # Write header
                header = [
                    "record_error_id",
                    "identifier",
                    "title",
                    "harvest_record_id",
                    "record_error_type",
                    "message",
                    "date_created",
                ]
                csv_writer.writerow(header)

                # Process record errors in batches
                page = 0
                batch_size = 100

                while True:
                    batch_errors = db.get_harvest_record_errors_by_job(
                        job_id,
                        paginate=True,
                        page=page,
                        per_page=batch_size,
                    )

                    if not batch_errors:
                        break

                    for error, identifier, source_raw in batch_errors:
                        # Extract title from source_raw JSON
                        title = ""
                        if source_raw:
                            try:
                                title = json.loads(source_raw).get("title", "")
                            except (json.JSONDecodeError, AttributeError):
                                title = ""

                        row = [
                            str(error.id) if error.id else "",
                            str(identifier) if identifier else "",
                            str(title),
                            (
                                str(error.harvest_record_id)
                                if error.harvest_record_id
                                else ""
                            ),
                            str(error.type) if error.type else "",
                            str(error.message) if error.message else "",
                            str(error.date_created) if error.date_created else "",
                        ]
                        csv_writer.writerow(row)

                    page += 1

                    # If we got fewer results than batch_size, we're done
                    if len(batch_errors) < batch_size:
                        break

            temp_file.close()

            # Create a generator that reads the file in chunks
            def generate_file_chunks():
                try:
                    with open(temp_file_path, "r") as f:
                        while True:
                            chunk = f.read(8192)  # Read in 8KB chunks
                            if not chunk:
                                break
                            yield chunk
                finally:
                    # Clean up the temporary file
                    try:
                        os.unlink(temp_file_path)
                    except OSError:
                        pass

            # Create streaming response
            response = Response(
                generate_file_chunks(),
                mimetype="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename={job_id}_{error_type}_errors.csv",
                    "Content-Type": "text/csv",
                },
            )

            return response

        except Exception as e:
            temp_file.close()
            try:
                os.unlink(temp_file_path)
            except OSError:
                pass
            raise e

    except Exception as e:
        logger.error(f"Error in download_harvest_errors_by_job: {repr(e)}")
        return "Error generating error report", 500


# Records
## Get record
@main.route("/harvest_record/<record_id>", methods=["GET"])
def get_harvest_record(record_id):
    record = db.get_harvest_record(record_id)
    return db._to_dict(record) if record else (STATUS_STRINGS_ENUM["404"], 404)


## Get records source raw
@main.route("/harvest_record/<record_id>/raw", methods=["GET"])
def get_harvest_record_raw(record_id=None):
    record = db.get_harvest_record(record_id)
    if record:
        try:
            # if this fails, it's not JSON, but possibly XML
            source_raw_json = json.loads(record.source_raw)
            return source_raw_json, 200
        except json.JSONDecodeError:
            return record.source_raw, 200
    else:
        return {"error": STATUS_STRINGS_ENUM["404"]}, 404


## Add record
@main.route("/harvest_record/add", methods=["POST", "GET"])
@login_required
def add_harvest_record():
    if request.is_json:
        record = db.add_harvest_record(request.json)
        if record:
            return {"message": f"Added new record with ID: {record.id}"}, 200
        else:
            return {"error": "Failed to add harvest record."}, 400
    else:
        return {"Please provide harvest record with json format."}


### Get record errors by record id
@main.route("/harvest_record/<record_id>/errors", methods=["GET"])
def get_all_harvest_record_errors(record_id: str) -> list:
    try:
        record_errors = db.get_harvest_record_errors_by_record(record_id)
        return (
            db._to_dict(record_errors)
            if record_errors
            else (STATUS_STRINGS_ENUM["404"], 404)
        )
    except Exception:
        return "Please provide a valid record_id"


## Harvest Error
### Get error by id
@main.route("/harvest_error/<error_id>", methods=["GET"])
def get_harvest_error(error_id: str = None) -> dict:
    # retrieves the given error ( either job or record )
    try:
        error = db.get_harvest_error(error_id)
        return db._to_dict(error) if error else (STATUS_STRINGS_ENUM["404"], 404)
    except Exception:
        return "Please provide a valid record_id"


@main.route("/metrics/", methods=["GET"])
def view_metrics():
    """Render index page with recent harvest jobs."""
    current_time = get_datetime()
    start_time = current_time - timedelta(days=7)
    time_filter = f"date_created >= '{start_time.isoformat()}' AND date_created <= '{current_time}'"

    # Add filtering parameters
    source_filter = request.args.get("source_filter", "").strip()
    status_filter = request.args.get("status_filter", "").strip()
    
    # Build filter string
    if source_filter:
        time_filter += f" AND harvest_source_id = '{source_filter}'"
    if status_filter:
        time_filter += f" AND status = '{status_filter}'"

    # Handle sorting
    sort_by = request.args.get("sort_by", "date_created")
    sort_order = request.args.get("sort_order", "desc")
    
    # Validate sort parameters
    valid_sort_columns = ["date_created", "records_added", "records_updated", "records_deleted", "records_errored"]
    if sort_by not in valid_sort_columns:
        sort_by = "date_created"
    if sort_order not in ["asc", "desc"]:
        sort_order = "desc"

    htmx_vars = {
        "target_div": "#paginated__harvest-jobs",
        "endpoint_url": "/metrics",
    }

    count = db.pget_harvest_jobs(
        facets=time_filter,
        count=True,
    )

    pagination = Pagination(
        count=count,
        current=request.args.get("page", 1, type=convert_to_int),
    )

    if htmx:
        jobs = db.pget_harvest_jobs(
            facets=time_filter,
            page=pagination.db_current,
            per_page=pagination.per_page,
            order_by=sort_order,
            sort_field=sort_by,
        )
        data = {
            "jobs": jobs,
            "htmx_vars": htmx_vars,
        }
        return render_block(
            "metrics_dashboard.html",
            "htmx_paginated",
            data=data,
            pagination=pagination.to_dict(),
        )
    else:
        jobs = db.pget_harvest_jobs(
            facets=time_filter,
            page=pagination.db_current,
            per_page=pagination.per_page,
            order_by=sort_order,
            sort_field=sort_by,
        )
        errors_time_filter = f"harvest_job_error.date_created >= '{start_time.isoformat()}' AND harvest_job_error.date_created <= '{current_time}'"
        failures = db.pget_harvest_job_errors(
            facets=errors_time_filter + " AND type = 'FailedJobCleanup'",
            order_by="desc",
        )
        
        # Get available sources and statuses for filter dropdowns
        available_sources = db.get_all_harvest_sources()
        available_statuses = ["new", "in_progress", "complete", "error"]  # Common statuses
        
        data = {
            "htmx_vars": htmx_vars,
            "jobs": jobs,
            "new_jobs_in_past": db.get_new_harvest_jobs_in_past(),
            "failures": failures,
            "current_time": current_time,
            "window_start": start_time,
            "available_sources": available_sources,
            "available_statuses": available_statuses,
        }
        return render_template(
            "metrics_dashboard.html",
            pagination=pagination.to_dict(),
            data=data,
        )


# Builder Query for JSON feed
@main.route("/organizations/", methods=["GET"])
@main.route("/harvest_sources/", methods=["GET"])
@main.route("/harvest_records/", methods=["GET"])
@main.route("/harvest_jobs/", methods=["GET"])
@main.route("/harvest_job_errors/", methods=["GET"])
@main.route("/harvest_record_errors/", methods=["GET"])
def json_builder_query():
    job_id = request.args.get("harvest_job_id")
    source_id = request.args.get("harvest_source_id")
    facets = request.args.get("facets", default="")

    if job_id:
        facets += f", harvest_job_id = '{job_id}'"
    if source_id:
        facets += f", harvest_source_id = '{source_id}'"

    model = escape(request.path).replace("/", "")
    try:
        res = db.pget_db_query(
            model=model,
            page=request.args.get("page", type=convert_to_int),
            per_page=request.args.get("per_page", type=convert_to_int),
            paginate=request.args.get("paginate", type=is_it_true),
            count=request.args.get("count", type=is_it_true),
            order_by=request.args.get("order_by"),
            facets=facets,
        )
        if not res:
            return f"No {model} found for this query", 404
        elif isinstance(res, int):
            # in case we are just returning a count from db query
            return {"count": res, "type": model}
        else:
            return db._to_dict(res)
    except Exception as e:
        logger.info(f"Failed json_builder_query :: {repr(e)} ")
        return "Error with query", 400


def register_routes(app):
    app.register_blueprint(main)
    app.register_blueprint(auth)
    app.register_blueprint(user)
    app.register_blueprint(org)
    app.register_blueprint(source)
    app.register_blueprint(job)
    app.register_blueprint(api)
    app.register_blueprint(testdata)
