import uuid
from controller.source import db
from . import bp
from flask import request

def valid(x):
    return x

@bp.route('/create/', methods=['GET'])
def create():
    name  = request.args.get('name', None)
    url  = request.args.get('url', None)
    if valid(name) + valid(url):
        source_id = str(uuid.uuid4())
        # result = create_dataset_record(source_id, name, url)
        db[source_id] = {'name': name, 'url': url}
        return {'data': db[source_id], 'source_id': source_id}

    else:
        # respond with whether name or url was invalid
        pass