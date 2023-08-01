from controller.dataset import db
from . import bp

@bp.route('/view/<id>', methods=['GET'])
def dataset_view(id):
    return db[id]