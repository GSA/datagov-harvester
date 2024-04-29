from flask import Blueprint, flash, jsonify, redirect, render_template, request

from .forms import HarvestSourceForm, OrganizationForm
from .interface import HarvesterDBInterface

mod = Blueprint("harvest", __name__)
db = HarvesterDBInterface()


# Helper Functions
def make_new_source(form):
    return {
        "name": form.name.data,
        "notification_emails": form.notification_emails.data.replace("\r\n", ", "),
        "frequency": form.frequency.data,
        "user_requested_frequency": form.frequency.data,
        "url": form.url.data,
        "schema_type": form.schema_type.data,
        "source_type": form.source_type.data,
        "organization_id": form.organization_id.data,
    }


def make_new_org(form):
    return {"name": form.name.data, "logo": form.logo.data}


# Routes
@mod.route("/", methods=["GET"])
def index():
    return render_template("index.html")


## Organizations
@mod.route("/organization/add", methods=["POST", "GET"])
def add_organization():
    form = OrganizationForm()
    if request.is_json:
        org = db.add_organization(request.json)
        if org:
            return jsonify({"message": f"Added new organization with ID: {org.id}"})
        else:
            return jsonify({"error": "Failed to add organization."}), 400
    else:
        form = OrganizationForm()
        if form.validate_on_submit():
            new_org = {"name": form.name.data, "logo": form.logo.data}
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
@mod.route("/organization/<org_id>", methods=["GET", "POST"])
def get_organization(org_id=None):
    if org_id:
        org = db.get_organization(org_id)
        return render_template(
            "view_data.html",
            data=org,
            action="View",
            data_type="Organization",
            org_id=org_id,
        )
    else:
        org = db.get_all_organizations()
    return jsonify(org)


@mod.route("/organization", methods=["GET"])
@mod.route("/organization/edit/<org_id>", methods=["GET", "POST"])
def edit_organization(org_id=None):
    if org_id:
        org = db.get_organization(org_id)
        form = OrganizationForm(data=org)
        if form.validate_on_submit():
            new_org_data = make_new_org(form)
            org = db.update_organization(org_id, new_org_data)
            if org:
                flash(f"Updated org with ID: {org['id']}")
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
    else:
        org = db.get_all_organizations()
    return org


## TODO: DO WE NEED THIS?
# @mod.route("/organization/<org_id>", methods=["DELETE"])
# def delete_organization(org_id):
#     result = db.delete_organization(org_id)
#     return result


@mod.route("/organization/delete/<org_id>", methods=["POST"])
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
@mod.route("/harvest_source/add", methods=["POST", "GET"])
def add_harvest_source():
    form = HarvestSourceForm()
    organizations = db.get_all_organizations()
    organization_choices = [
        (str(org["id"]), f'{org["name"]} - {org["id"]}') for org in organizations
    ]
    form.organization_id.choices = organization_choices

    if request.is_json:
        org = db.add_harvest_source(request.json)
        if org:
            return jsonify({"message": f"Added new harvest source with ID: {org.id}"})
        else:
            return jsonify({"error": "Failed to add harvest source."}), 400
    else:
        if form.validate_on_submit():
            new_source = make_new_source(form)
            source = db.add_harvest_source(new_source)
            if source:
                flash(f"Updated source with ID: {source.id}")
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


@mod.route("/harvest_source/", methods=["GET"])
@mod.route("/harvest_source/<source_id>", methods=["GET", "POST"])
def get_harvest_source(source_id=None):
    if source_id:
        source = db.get_harvest_source(source_id)
        return render_template(
            "view_data.html",
            data=source,
            action="View",
            data_type="Harvest Source",
            source_id=source_id,
        )
    else:
        source = db.get_all_harvest_sources()
    return jsonify(source)


@mod.route("/harvest_source/edit/<source_id>", methods=["GET", "POST"])
def edit_harvest_source(source_id=None):
    if source_id:
        source = db.get_harvest_source(source_id)
        organizations = db.get_all_organizations()
        organization_choices = [
            (str(org["id"]), f'{org["name"]} - {org["id"]}') for org in organizations
        ]
        form = HarvestSourceForm(data=source)
        form.organization_id.choices = organization_choices
        if form.validate_on_submit():
            new_source_data = make_new_source(form)
            source = db.update_harvest_source(source_id, new_source_data)
            if source:
                flash(f"Updated source with ID: {source['id']}")
            else:
                flash("Failed to update harvest source.")
            return redirect(f"/harvest_source/{source['id']}")
        return render_template(
            "edit_data.html",
            form=form,
            action="Edit",
            data_type="Harvest Source",
            button="Update",
            source_id=source_id,
        )

    organization_id = request.args.get("organization_id")
    if organization_id:
        source = db.get_harvest_source_by_org(organization_id)
        if not source:
            return "No harvest sources found for this organization", 404
    else:
        source = db.get_all_harvest_sources()
    return jsonify(source)


@mod.route("/harvest_source/harvest/<source_id>", methods=["GET"])
def trigger_harvest_source(source_id):
    job = db.add_harvest_job(
        {"harvest_source_id": source_id, "status": "pending_manual"}
    )
    if job:
        flash(f"Triggered harvest of source with ID: {source_id}")
    else:
        flash("Failed to add harvest job.")
    return redirect(f"/harvest_source/{source_id}")


@mod.route("/harvest_source/delete/<source_id>", methods=["POST"])
def delete_harvest_source(source_id):
    try:
        result = db.delete_harvest_source(source_id)
        if result:
            flash(f"Triggered delete of source with ID: {source_id}")
            return {"message": "success"}
        else:
            raise Exception()
    except Exception:
        flash("Failed to delete harvest source")
        return {"message": "failed"}


## Harvest Job
@mod.route("/harvest_job/add", methods=["POST"])
def add_harvest_job():
    if request.is_json:
        job = db.add_harvest_job(request.json)
        if job:
            return jsonify({"message": f"Added new harvest job with ID: {job.id}"})
        else:
            return jsonify({"error": "Failed to add harvest job."}), 400
    else:
        return jsonify({"Please provide harvest job with json format."})


@mod.route("/harvest_job/", methods=["GET"])
@mod.route("/harvest_job/<job_id>", methods=["GET"])
def get_harvest_job(job_id=None):
    try:
        if job_id:
            job = db.get_harvest_job(job_id)
            return jsonify(job) if job else ("Not Found", 404)

        source_id = request.args.get("harvest_source_id")
        if source_id:
            job = db.get_harvest_job_by_source(source_id)
            if not job:
                return "No harvest jobs found for this harvest source", 404
        else:
            job = db.get_all_harvest_jobs()

        return jsonify(job)
    except Exception:
        return "Please provide correct job_id or harvest_source_id"


@mod.route("/harvest_job/<job_id>", methods=["PUT"])
def update_harvest_job(job_id):
    result = db.update_harvest_job(job_id, request.json)
    return result


@mod.route("/harvest_job/<job_id>", methods=["DELETE"])
def delete_harvest_job(job_id):
    result = db.delete_harvest_job(job_id)
    return result


## Harvest Error
@mod.route("/harvest_error/add", methods=["POST", "GET"])
def add_harvest_error():
    if request.is_json:
        error = db.add_harvest_error(request.json)
        if error:
            return jsonify({"message": f"Added new harvest error with ID: {error.id}"})
        else:
            return jsonify({"error": "Failed to add harvest error."}), 400
    else:
        return jsonify({"Please provide harvest error with json format."})


@mod.route("/harvest_error/", methods=["GET"])
@mod.route("/harvest_error/<error_id>", methods=["GET"])
def get_harvest_error(error_id=None):
    try:
        if error_id:
            error = db.get_harvest_error(error_id)
            return jsonify(error) if error else ("Not Found", 404)

        job_id = request.args.get("harvest_job_id")
        if job_id:
            error = db.get_harvest_error_by_job(job_id)
            if not error:
                return "No harvest errors found for this harvest job", 404
        else:
            # for test, will remove later
            error = db.get_all_harvest_errors()

        return jsonify(error)
    except Exception:
        return "Please provide correct error_id or harvest_job_id"


## Harvest Record
@mod.route("/harvest_record/add", methods=["POST", "GET"])
def add_harvest_record():
    if request.is_json:
        record = db.add_harvest_record(request.json)
        if record:
            return jsonify({"message": f"Added new record with ID: {record.id}"})
        else:
            return jsonify({"error": "Failed to add harvest record."}), 400
    else:
        return jsonify({"Please provide harvest record with json format."})


@mod.route("/harvest_record/", methods=["GET"])
@mod.route("/harvest_record/<record_id>", methods=["GET"])
def get_harvest_record(record_id=None):
    try:
        if record_id:
            record = db.get_harvest_record(record_id)
            return jsonify(record) if record else ("Not Found", 404)

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
            # for test, will remove later
            record = db.get_all_harvest_records()

        return jsonify(record)
    except Exception:
        return "Please provide correct record_id or harvest_job_id"


## Test interface, will remove later
# TODO: remove / improve
@mod.route("/get_data_sources", methods=["GET"])
def get_data_sources():
    source = db.get_all_harvest_sources()
    org = db.get_all_organizations()
    return render_template("get_data_sources.html", sources=source, organizations=org)


def register_routes(app):
    app.register_blueprint(mod)
