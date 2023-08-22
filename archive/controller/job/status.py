from controller.job import bp, db


@bp.route("/status/<id>", methods=["GET"])
def dataset_view(id):
    result = db[id]
    return {"job_id": id, "job_status": result["job_status"]}
