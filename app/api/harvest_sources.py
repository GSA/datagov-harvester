from flask import flash, jsonify, make_response, redirect, request, url_for

from app import deps
from app.deps import (
    JSON_NOT_FOUND,
    _log_mutation,
    logger,
    login_required,
    valid_id_required,
)
from harvester.utils.general_utils import is_valid_uuid4

from . import api


@api.post("/harvest_source/add")
@api.doc(hide=True)
@login_required
def add_harvest_source_api():
    try:
        source = deps.db.add_harvest_source(request.json)
        job_message = deps.load_manager.schedule_first_job(source.id)
        if source and job_message:
            _log_mutation(
                "create",
                "harvest_source",
                source.id,
                organization_id=source.organization_id,
                source_name=source.name,
            )
            return make_response(
                jsonify(
                    {
                        "message": (
                            f"Added new harvest source with ID: {source.id}. "
                            f"{job_message}"
                        )
                    }
                ),
                200,
            )
    except Exception as e:
        message = "Failed to add harvest source."
        logger.error(f"{message} :: {repr(e)}")
        return make_response(jsonify({"message": message}), 500)


@api.post("/harvest_source/edit/<source_id>")
@api.doc(hide=True)
@login_required
@valid_id_required
def edit_harvest_source_api(source_id: str):
    updated_source = deps.db.update_harvest_source(source_id, request.json)
    job_message = deps.load_manager.schedule_first_job(updated_source.id)

    if updated_source and job_message:
        _log_mutation(
            "edit",
            "harvest_source",
            updated_source.id,
            organization_id=updated_source.organization_id,
            source_name=updated_source.name,
        )
        return {
            "message": f"Updated source with ID: {updated_source.id}. {job_message}"
        }, 200
    else:
        return {"error": "Failed to update harvest source"}, 400


@api.route("/harvest_source/<source_id>", methods=["DELETE"])
@api.doc(hide=True)
@login_required
@valid_id_required
def delete_harvest_source(source_id):
    try:
        message, status = deps.db.delete_harvest_source(source_id)
        _log_mutation("delete", "harvest_source", source_id, status=status)
        return make_response(jsonify({"message": message}), status)
    except Exception as e:
        message = f"Failed to delete harvest source :: {repr(e)}"
        logger.error(message)
        return make_response(jsonify({"message": message}), 500)


@api.route("/harvest_source/harvest/<source_id>/<job_type>", methods=["GET"])
@api.doc(hide=True)
@login_required
def trigger_harvest_source(source_id, job_type):
    if not is_valid_uuid4(source_id):
        return JSON_NOT_FOUND()
    if job_type not in ["harvest", "force_harvest", "clear", "validate"]:
        return JSON_NOT_FOUND()
    message = deps.load_manager.trigger_manual_job(source_id, job_type)
    flash(message)
    return redirect(url_for("main.view_harvest_source", source_id=source_id))
