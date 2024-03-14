from flask import Blueprint, request, render_template
from .interface import HarvesterDBInterface
from tests.database.data import new_org, new_source, new_job, new_error

mod = Blueprint('harvest', __name__)
db = HarvesterDBInterface()

@mod.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@mod.route('/add_org', methods=['POST', 'GET'])
def add_organization():
    org=db.add_organization(new_org)
    return(f"Added new organization with ID: {org.id}")

@mod.route('/add_source', methods=['POST', 'GET'])
def add_harvest_source():
    org_id = request.args.get('org_id', None)
    if org_id is None:
        return 'Please provide org_id: /add_source?org_id=xxx'
    else:
        source=db.add_harvest_source(new_source, org_id)
        return(f"Added new source with ID: {source.id}")

@mod.route('/add_job', methods=['POST', 'GET'])
def add_harvest_job():
    source_id = request.args.get('source_id', None)
    if source_id is None:
        return 'Please provide source_id: /add_job?source_id=xxx'
    else:
        job=db.add_harvest_job(new_job, source_id)
        return(f"Added new job with ID: {job.id}")

@mod.route('/add_error', methods=['POST', 'GET'])
def add_harvest_error():
    job_id = request.args.get('job_id', None)
    if job_id is None:
        return 'Please provide job_id: /add_error?job_id=xxx'
    else:
        err=db.add_harvest_error(new_error, job_id)
        return(f"Added new error with ID: {err.id}")

@mod.route('/organizations', methods=['GET'])
def get_all_organizations():
    result = db.get_all_organizations()
    return result
   
@mod.route('/harvest_sources', methods=['GET'])
def get_all_harvest_sources():
    result = db.get_all_harvest_sources()
    return result

@mod.route('/harvest_jobs', methods=['GET'])
def get_all_harvest_jobs():
    result = db.get_all_harvest_jobs()
    return result

@mod.route('/harvest_errors_by_job/<job_id>', methods=['GET'])
def get_all_harvest_errors_by_job(job_id):
    try:
        result = db.get_all_harvest_errors_by_job(job_id)
        return result
    except Exception:
        return " provide job_id"
    
@mod.route('/harvest_source/<source_id>', methods=['GET'])
def get_harvest_source(source_id):
    try:
        result = db.get_harvest_source(source_id)
        return result
    except Exception:
        return " provide source_id"
    
@mod.route('/harvest_job/<job_id>', methods=['GET'])
def get_harvest_job(job_id):
    try:
        result = db.get_harvest_job(job_id)
        return result
    except Exception:
        return "provide job_id"

@mod.route('/harvest_error/<error_id>', methods=['GET'])
def get_harvest_error(error_id):
    try:
        result = db.get_harvest_error(error_id)
        return result
    except Exception:
        return "provide error_id"


@mod.route("/harvests/<string:source_id>", methods=["PUT"])
def update_harvest_source():
    source_id = request.args.get("source_id", None)
    if source_id is None:
        return "Please provide a source_id"
    else:
        result = db.update_harvest_source(source_id, request.json)
        return result

def register_routes(app):
    app.register_blueprint(mod)
