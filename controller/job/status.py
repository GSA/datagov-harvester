from controller.job import db
from . import bp

@bp.route('/status/<id>', methods=['GET'])
def dataset_view(id):
    result = db[id]
    return {'job_id': id, 'job_status': result['job_status']}