import logging
import os
import secrets
import time
import uuid
from functools import wraps

import click
import jwt
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from dotenv import load_dotenv
from flask import Blueprint, flash, redirect, render_template, request, session, url_for

from app.scripts.load_manager import schedule_first_job, trigger_manual_job
from database.interface import HarvesterDBInterface

from .forms import HarvestSourceForm, OrganizationForm

logger = logging.getLogger("harvest_admin")

user = Blueprint("user", __name__)
mod = Blueprint("harvest", __name__)
db = HarvesterDBInterface()

# Login authentication
load_dotenv()
CLIENT_ID = os.getenv("CLIENT_ID")
REDIRECT_URI = os.getenv("REDIRECT_URI")
ISSUER = os.getenv("ISSUER")
AUTH_URL = ISSUER + "/openid_connect/authorize"
TOKEN_URL = ISSUER + "/api/openid_connect/token"


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "user" not in session:
            session["next"] = request.url
            return redirect(url_for("harvest.login"))
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
        "iat": now,
    }

    return jwt.encode(payload, private_key, algorithm="RS256")


@mod.route("/login")
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


@mod.route("/logout")
def logout():
    session.pop("user", None)
    return redirect(url_for("harvest.index"))


@mod.route("/callback")
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
            return redirect(url_for("harvest.index"))
    else:
        flash("Please request registration from the admin before proceeding.")
        return redirect(url_for("harvest.index"))


# User management
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


# Helper Functions
def make_new_source_contract(form):
    return {
        "name": form.name.data,
        "notification_emails": form.notification_emails.data,
        "frequency": form.frequency.data,
        "url": form.url.data,
        "schema_type": form.schema_type.data,
        "source_type": form.source_type.data,
        "organization_id": form.organization_id.data,
    }


def make_new_org_contract(form):
    return {"name": form.name.data, "logo": form.logo.data}


# Routes
@mod.route("/", methods=["GET"])
def index():
    return render_template("index.html")


## Organizations
# Add Org
@mod.route("/organization/add", methods=["POST", "GET"])
@login_required
def add_organization():
    form = OrganizationForm()
    if request.is_json:
        org = db.add_organization(request.json)
        if org:
            return {"message": f"Added new organization with ID: {org.id}"}
        else:
            return {"error": "Failed to add organization."}, 400
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
            return redirect("/")
    return render_template(
        "edit_data.html",
        form=form,
        action="Add",
        data_type="Organization",
        button="Submit",
    )


@mod.route("/organization/", methods=["GET"])
@mod.route("/organizations/", methods=["GET"])
def view_organizations():
    organizations = db.get_all_organizations()
    if request.args.get("type") and request.args.get("type") == "json":
        return db._to_dict(organizations)
    else:
        data = {"organizations": organizations}
        return render_template("view_org_list.html", data=data)


@mod.route("/organization/<org_id>", methods=["GET"])
def view_org_data(org_id: str):
    org = db.get_organization(org_id)
    sources = db.get_harvest_source_by_org(org_id)
    future_harvest_jobs = {}
    for source in sources:
        job = db.get_new_harvest_jobs_by_source_in_future(source.id)
        if len(job):
            future_harvest_jobs[source.id] = job[0].date_created
    harvest_jobs = {}
    for source in sources:
        job = db.get_first_harvest_jobs_by_filter(
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
    return render_template("view_org_data.html", data=data)


# Edit Org
@mod.route("/organization/config/edit/<org_id>", methods=["GET", "POST"])
@login_required
def edit_organization(org_id):
    org = db._to_dict(db.get_organization(org_id))
    form = OrganizationForm(data=org)
    if form.validate_on_submit():
        new_org_data = make_new_org_contract(form)
        org = db.update_organization(org_id, new_org_data)
        if org:
            flash(f"Updated org with ID: {org.id}")
        else:
            flash("Failed to update organization.")
        return redirect(f"/organization/{org_id}")
    return render_template(
        "edit_data.html",
        form=form,
        action="Edit",
        data_type="Organization",
        button="Update",
    )


# Delete Org
@mod.route("/organization/config/delete/<org_id>", methods=["POST"])
@login_required
def delete_organization(org_id):
    try:
        result = db.delete_organization(org_id)
        if result:
            flash(f"Triggered delete of organization with ID: {org_id}")
            return {"message": "success"}
        else:
            raise Exception()
    except Exception:
        flash("Failed to delete harvest source")
        return {"message": "failed"}


## Harvest Source
# Add Source
@mod.route("/harvest_source/add", methods=["POST", "GET"])
@login_required
def add_harvest_source():
    form = HarvestSourceForm()
    organizations = db.get_all_organizations()
    organization_choices = [
        (str(org.id), f"{org.name} - {org.id}") for org in organizations
    ]
    form.organization_id.choices = organization_choices

    if request.is_json:
        source = db.add_harvest_source(request.json)
        job_message = schedule_first_job(source.id)
        if source and job_message:
            return {
                "message": f"Added new harvest source with ID: {source.id}. {job_message}"
            }
        else:
            return {"error": "Failed to add harvest source."}, 400
    else:
        if form.validate_on_submit():
            new_source = make_new_source_contract(form)
            source = db.add_harvest_source(new_source)
            job_message = schedule_first_job(source.id)
            if source and job_message:
                flash(f"Updated source with ID: {source.id}. {job_message}")
            else:
                flash("Failed to add harvest source.")
            return redirect("/")
    return render_template(
        "edit_data.html",
        form=form,
        action="Add",
        data_type="Harvest Source",
        button="Submit",
    )


@mod.route("/harvest_source/<source_id>", methods=["GET"])
def view_harvest_source_data(source_id: str):
    source = db.get_harvest_source(source_id)
    jobs = db.get_all_harvest_jobs_by_filter(
        {"harvest_source_id": source.id, "status": "complete"}
    )
    next_job = "N/A"
    future_jobs = db.get_new_harvest_jobs_by_source_in_future(source.id)
    if len(future_jobs):
        next_job = future_jobs[0].date_created
    chartdata = {
        "labels": [job.date_finished for job in jobs],
        "datasets": [
            {
                "label": "Added",
                "data": [job.records_added for job in jobs],
                "borderColor": "green",
                "backgroundColor": "green",
            },
            {
                "label": "Deleted",
                "data": [job.records_deleted for job in jobs],
                "borderColor": "black",
                "backgroundColor": "black",
            },
            {
                "label": "Errored",
                "data": [job.records_errored for job in jobs],
                "borderColor": "red",
                "backgroundColor": "red",
            },
            {
                "label": "Igrored",
                "data": [job.records_ignored for job in jobs],
                "borderColor": "grey",
                "backgroundColor": "grey",
            },
        ],
    }
    data = {
        "harvest_source": source,
        "harvest_source_dict": db._to_dict(source),
        "harvest_jobs": jobs,
        "chart": chartdata,
        "next_job": next_job,
    }
    return render_template("view_source_data.html", data=data)


@mod.route("/harvest_source/", methods=["GET"])
@mod.route("/harvest_sources/", methods=["GET"])
def view_harvest_sources():
    sources = db.get_all_harvest_sources()
    data = {"harvest_sources": sources}
    return render_template("view_source_list.html", data=data)


# Edit Source
@mod.route("/harvest_source/config/edit/<source_id>", methods=["GET", "POST"])
@login_required
def edit_harvest_source(source_id: str):
    if source_id:
        source = db.get_harvest_source(source_id)
        organizations = db.get_all_organizations()
        if source and organizations:
            organization_choices = [
                (str(org["id"]), f'{org["name"]} - {org["id"]}')
                for org in db._to_dict(organizations)
            ]
            source.notification_emails = ", ".join(source.notification_emails)
            form = HarvestSourceForm(data=db._to_dict(source))
            form.organization_id.choices = organization_choices
            if form.validate_on_submit():
                new_source_data = make_new_source_contract(form)
                source = db.update_harvest_source(source_id, new_source_data)
                job_message = schedule_first_job(source.id)
                if source and job_message:
                    flash(f"Updated source with ID: {source.id}. {job_message}")
                else:
                    flash("Failed to update harvest source.")
                return redirect(
                    url_for("harvest.view_harvest_source_data", source_id=source.id)
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
            return redirect(url_for("harvest.view_harvest_sources"))

    organization_id = request.args.get("organization_id")
    if organization_id:
        source = db.get_harvest_source_by_org(organization_id)
        if not source:
            return "No harvest sources found for this organization", 404
    else:
        source = db.get_all_harvest_sources()
    return db._to_dict(source)


# Delete Source
@mod.route("/harvest_source/config/delete/<source_id>", methods=["POST"])
def delete_harvest_source(source_id):
    try:
        result = db.delete_harvest_source(source_id)
        if result:
            flash(f"Triggered delete of source with ID: {source_id}")
            return {"message": "success"}
        else:
            raise Exception()
    except Exception as e:
        logger.error(f"Failed to delete harvest source :: {repr(e)}")
        flash("Failed to delete harvest source")
        return {"message": "failed"}


# Trigger Harvest
@mod.route("/harvest_source/harvest/<source_id>", methods=["GET"])
def trigger_harvest_source(source_id):
    message = trigger_manual_job(source_id)
    flash(message)
    return redirect(f"/harvest_source/{source_id}")


## Harvest Job
# Add Job
@mod.route("/harvest_job/add", methods=["POST"])
def add_harvest_job():
    if request.is_json:
        job = db.add_harvest_job(request.json)
        if job:
            return {"message": f"Added new harvest job with ID: {job.id}"}
        else:
            return {"error": "Failed to add harvest job."}, 400
    else:
        return {"Please provide harvest job with json format."}


# Get Job
@mod.route("/harvest_job/", methods=["GET"])
@mod.route("/harvest_job/<job_id>", methods=["GET"])
def get_harvest_job(job_id=None):
    if job_id:
        job = db.get_harvest_job(job_id)
        record_errors = db.get_harvest_record_errors_by_job(job_id)
        if request.args.get("type") and request.args.get("type") == "json":
            return db._to_dict(job) if job else ("Not Found", 404)
        else:
            data = {
                "harvest_job": job,
                "harvest_job_dict": db._to_dict(job),
                "record_errors": db._to_dict(record_errors),
            }
            return render_template("view_job_data.html", data=data)

    source_id = request.args.get("harvest_source_id")
    if source_id:
        job = db.get_harvest_job_by_source(source_id)
        if not job:
            return "No harvest jobs found for this harvest source", 404
    else:
        job = db.get_all_harvest_jobs()
        return db._to_dict(job)


# Update Job
@mod.route("/harvest_job/<job_id>", methods=["PUT"])
def update_harvest_job(job_id):
    result = db.update_harvest_job(job_id, request.json)
    return db._to_dict(result)


# Delete Job
@mod.route("/harvest_job/<job_id>", methods=["DELETE"])
def delete_harvest_job(job_id):
    result = db.delete_harvest_job(job_id)
    return db._to_dict(result)


# Get Job Errors by Type
@mod.route("/harvest_job/<job_id>/errors/<error_type>", methods=["GET"])
def get_harvest_errors_by_job(job_id, error_type):
    try:
        match error_type:
            case "job":
                return db.get_harvest_job_errors_by_job(job_id)
            case "record":
                return db.get_harvest_record_errors_by_job(job_id)
    except Exception:
        return "Please provide correct job_id"


## Harvest Record
# Get record
@mod.route("/harvest_record/", methods=["GET"])
@mod.route("/harvest_record/<record_id>", methods=["GET"])
def get_harvest_record(record_id=None):
    if record_id:
        record = db.get_harvest_record(record_id)
        return db._to_dict(record) if record else ("Not Found", 404)

    job_id = request.args.get("harvest_job_id")
    source_id = request.args.get("harvest_source_id")
    if job_id:
        record = db.get_harvest_record_by_job(job_id)
        if not record:
            return "No harvest records found for this harvest job", 404
    elif source_id:
        record = db.get_harvest_record_by_source(source_id)
        if not record:
            return "No harvest records found for this harvest source", 404
    else:
        # TODO for test, will remove later
        record = db.get_all_harvest_records()

    return db._to_dict(record)


# Add record
@mod.route("/harvest_record/add", methods=["POST", "GET"])
def add_harvest_record():
    if request.is_json:
        record = db.add_harvest_record(request.json)
        if record:
            return {"message": f"Added new record with ID: {record.id}"}
        else:
            return {"error": "Failed to add harvest record."}, 400
    else:
        return {"Please provide harvest record with json format."}


# Get record errors by record id
@mod.route("/harvest_record/<record_id>/errors", methods=["GET"])
def get_all_harvest_record_errors(record_id: str) -> list:
    try:
        record_errors = db.get_harvest_errors_by_record(record_id)
        return db._to_dict(record_errors) if record_errors else ("Not Found", 404)
    except Exception:
        return "Please provide correct record_id"


## Harvest Error
# Get error by id
@mod.route("/harvest_error/", methods=["GET"])
@mod.route("/harvest_error/<error_id>", methods=["GET"])
def get_harvest_error(error_id: str = None) -> dict:
    # retrieves the given error ( either job or record )
    if error_id:
        error = db.get_harvest_error(error_id)
        return db._to_dict(error) if error else ("Not Found", 404)
    else:
        errors = db.get_all_harvest_errors()
        return db._to_dict(errors)


## Test interface, will remove later
# TODO: remove with completion of https://github.com/GSA/data.gov/issues/4741
@mod.route("/get_data_sources", methods=["GET"])
def get_data_sources():
    source = db.get_all_harvest_sources()
    org = db.get_all_organizations()
    return render_template("get_data_sources.html", sources=source, organizations=org)


## Test interface, will remove later
@mod.route("/delete_all_records", methods=["DELETE"])
def delete_all_records():
    db.delete_all_harvest_records()
    return "All harvest records deleted"


## Test interface, will remove later
@mod.route("/add_harvest_job_error", methods=["POST"])
def add_harvest_job_error():
    db.add_harvest_job_error(request.json)
    return "Added harvest job error"


## Test interface, will remove later
@mod.route("/add_harvest_record_error", methods=["POST"])
def add_harvest_record_error():
    err = db.add_harvest_record_error(request.json)
    return db._to_dict(err)


def register_routes(app):
    app.register_blueprint(mod)
    app.register_blueprint(user)
