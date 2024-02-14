from flask import Flask, request, jsonify
import sys
from harvester.database.interface import HarvesterDBInterface
from harvester.database import init_db

app = Flask(__name__)
db = HarvesterDBInterface()

new_source = {
    'name': 'Example Harvest Source',
    'notification_emails': ['admin@example.com'],
    'organization_name': 'Example Organization',
    'frequency': 'daily',
    'config': '{"url": "http://example.com", "schema_validation_type": "strict"}'
}

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

@app.route('/add_job', methods=['POST'])
def add_job():
    
    pass

@app.route('/add_error', methods=['POST'])
def add_error():
    
    pass

@app.route('/get_all_sources', methods=['GET'])
def get_all_sources():
    result = db.get_all_harvest_sources()
    return result

@app.route('/get_source/<id>', methods=['GET'])
def get_source(id):
    result = db.get_harvest_source(id)
    return result.name


@app.route('/get_all_jobs', methods=['GET'])
def get_all_jobs():
    
    pass

@app.route('/get_job/<id>', methods=['GET'])
def get_job(id):
    
    pass

@app.route('/get_all_errors', methods=['GET'])
def get_all_errors():
    
    pass

@app.route('/get_error/<id>', methods=['GET'])
def get_error(id):
    
    pass

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=sys.argv[1], debug=True)
