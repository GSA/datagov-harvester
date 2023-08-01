from controller.dataset import bp, db


@bp.route("/view/<id>", methods=["GET"])
def dataset_view(id):
    return db[id]
