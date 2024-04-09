from flask import Blueprint, request, render_template, jsonify
from .interface import HarvesterDBInterface
from .forms import HarvestSourceForm, OrganizationForm

mod = Blueprint("harvest", __name__)
db = HarvesterDBInterface()

@mod.route("/", methods=["GET"])
def index():
    return render_template("index.html")

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
            new_org = {
                "name": form.name.data,
                "logo": form.logo.data
            }
            org=db.add_organization(new_org)
            if org:
                return f"Added new organization with ID: {org.id}"
            else:
                return "Failed to add organization."
    return render_template("org_form.html", form=form)

@mod.route("/organization", methods=["GET"])
@mod.route("/organization/<org_id>", methods=["GET"])
def get_organization(org_id=None):
    if org_id:
        org = db.get_organization(org_id)
        return jsonify(org) if org else ("Not Found", 404)
    else:
        org = db.get_all_organizations()
    return org

@mod.route("/organization/<org_id>", methods=["PUT"])
def update_organization(org_id):
    result = db.update_organization(org_id, request.json)
    return result

@mod.route("/organization/<org_id>", methods=["DELETE"])
def delete_organization(org_id):
    result = db.delete_organization(org_id)
    return result

@mod.route("/harvest_source/add", methods=["POST", "GET"])
def add_harvest_source():
    form = HarvestSourceForm()
    organizations = db.get_all_organizations()
    organization_choices = [(str(org["id"]), f'{org["name"]} - {org["id"]}')
                            for org in organizations]
    form.organization_id.choices = organization_choices
    
    if request.is_json:
        org = db.add_harvest_source(request.json)
        if org:
            return jsonify({"message": f"Added new harvest source with ID: {org.id}"})
        else:
            return jsonify({"error": "Failed to add harvest source."}), 400
    else:
        if form.validate_on_submit():
            new_source = {
                "name": form.name.data,
                "notification_emails": form.emails.data.replace('\r\n', ', '),
                "frequency": form.frequency.data,
                "user_requested_frequency": form.frequency.data,
                "url": form.url.data,
                "schema_type": form.schema_type.data,
                "source_type": form.source_type.data,
                "organization_id": form.organization_id.data
            }
            source=db.add_harvest_source(new_source)
            if source:
                return f"Added new source with ID: {source.id}"
            else:
                return "Failed to add harvest source."
    return render_template("source_form.html", form=form, choices=organization_choices)

# test interface, will remove later
@mod.route("/get_harvest_source", methods=["GET"]) 
def get_all_harvest_sources():
    source = db.get_all_harvest_sources()
    org = db.get_all_organizations()
    return render_template("harvest_source.html", sources=source, organizations=org)

@mod.route("/harvest_source/", methods=["GET"])    
@mod.route("/harvest_source/<source_id>", methods=["GET"])
def get_harvest_source(source_id=None):
    if source_id:
        source = db.get_harvest_source(source_id)
        return jsonify(source) if source  else ("Not Found", 404)

    organization_id = request.args.get("organization_id")
    if organization_id:
        source = db.get_harvest_source_by_org(organization_id)
        if not source:
            return "No harvest sources found for this organization", 404
    else:
        source = db.get_all_harvest_sources()
    return jsonify(source)
        
@mod.route("/harvest_source/<source_id>", methods=["PUT"])
def update_harvest_source(source_id):
    result = db.update_harvest_source(source_id, request.json)
    return result

@mod.route("/harvest_source/<source_id>", methods=["DELETE"])
def delete_harvest_source(source_id):
    result = db.delete_harvest_source(source_id)
    return result

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



def register_routes(app):
    app.register_blueprint(mod)
