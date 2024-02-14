from flask import Flask, request, jsonify
import sys
from harvester.database.interface import HarvesterDBInterface
from harvester.database import init_db
from tests.database.data import new_source, new_job, new_error

app = Flask(__name__)
db = HarvesterDBInterface()

@app.route('/', methods=['GET'])
def index():
    return('testing for harvestDB')

@app.route('/init_db', methods=['GET'])
def create_tables():
    result = init_db.create_tables()
    return result

@app.route('/add_source', methods=['GET'])
def add_source():
    source=db.add_harvest_source(new_source)
    return(f"Added new source with ID: {source.id}")

@app.route('/add_job', methods=['GET'])
def add_job():
    source_id = request.args.get('source_id', None)
    job=db.add_harvest_job(new_job, source_id)
    return(f"Added new job with ID: {job.id}")

@app.route('/add_error', methods=['GET'])
def add_error():
    job_id = request.args.get('job_id', None)
    err=db.add_harvest_error(new_error, job_id)
    return(f"Added new error with ID: {err.id}")
    
@app.route('/get_all_sources', methods=['GET'])
def get_all_sources():
    result = db.get_all_harvest_sources()
    return result

@app.route('/get_source', methods=['GET'])
def get_source():
    id = request.args.get('id', None)
    result = db.get_harvest_source(id)
    return result

@app.route('/get_all_jobs', methods=['GET'])
def get_all_jobs():
    result = db.get_all_harvest_jobs()
    return result

@app.route('/get_job', methods=['GET'])
def get_job():
    id = request.args.get('id', None)
    result = db.get_harvest_job(id)
    return result

@app.route('/get_all_errors', methods=['GET'])
def get_all_errors():
    result = db.get_all_harvest_errors()
    return result

@app.route('/get_error', methods=['GET'])
def get_error():
    id = request.args.get('id', None)
    result = db.get_harvest_error(id)
    return result

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=sys.argv[1], debug=True)
