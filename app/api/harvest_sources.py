import csv
import json

from apiflask.schemas import FileSchema
from flask import (
    Response,
    flash,
    jsonify,
    make_response,
    redirect,
    request,
    stream_with_context,
    url_for,
)

from app import deps
from app.deps import (
    JSON_NOT_FOUND,
    _log_mutation,
    create_harvest_source,
    logger,
    login_required,
    valid_id_required,
)
from harvester.lib.source_delete import enqueue_harvest_source_delete
from harvester.utils.general_utils import is_valid_uuid4

from . import api


@api.post("/harvest_source/add")
@api.doc(hide=True)
@login_required
def add_harvest_source_api():
    try:
        _source, message, status = create_harvest_source(request.json)
        return make_response(jsonify({"message": message}), status)
    except Exception as e:
        message = "Failed to add harvest source."
        logger.error(f"{message} :: {repr(e)}")
        return make_response(jsonify({"message": message}), 500)


@api.post("/harvest_source/edit/<source_id>")
@api.doc(hide=True)
@login_required
@valid_id_required
def edit_harvest_source_api(source_id: str):
    existing_source = deps.db.get_harvest_source(source_id)
    old_frequency = existing_source.frequency if existing_source else None
    old_locked = existing_source.is_locked if existing_source else None
    updated_source = deps.db.update_harvest_source(source_id, request.json)
    job_message = ""
    if updated_source and updated_source.frequency != old_frequency:
        job_message = deps.load_manager.reschedule_next_run(updated_source.id)

    if updated_source:
        if old_locked != updated_source.is_locked:
            _log_mutation(
                "lock" if updated_source.is_locked else "unlock",
                "harvest_source",
                updated_source.id,
            )
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
        message, status = enqueue_harvest_source_delete(source_id, deps.db)
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
    message, category = deps.load_manager.trigger_manual_job(source_id, job_type)
    flash(message, category)
    return redirect(url_for("main.view_harvest_source", source_id=source_id))


@api.route("/harvest_source/<string:source_id>/warnings/download", methods=["GET"])
@api.doc(
    responses={
        200: {
            "description": "Outstanding record issues report as CSV file",
            "content": {"text/csv": {"schema": {"type": "string"}}},
        },
        404: {"description": "Source not found"},
    }
)
@api.output(FileSchema, content_type="text/csv")
@valid_id_required
def download_current_record_issues_by_source(source_id):
    try:

        class Echo:
            def write(self, value):
                return value

        csv_writer = csv.writer(Echo())

        def generate_csv():
            header = [
                "record_error_id",
                "identifier",
                "title",
                "harvest_record_id",
                "record_error_type",
                "severity",
                "message",
                "date_created",
            ]
            yield csv_writer.writerow(header)

            for error in deps.db.stream_current_record_issues_by_source(source_id):
                title = ""
                source_raw = error["source_raw"]
                if source_raw:
                    try:
                        title = json.loads(source_raw).get("title", "")
                    except (json.JSONDecodeError, AttributeError):
                        title = ""

                row = [
                    str(error["id"]) if error["id"] else "",
                    str(error["identifier"]) if error["identifier"] else "",
                    str(title),
                    (
                        str(error["harvest_record_id"])
                        if error["harvest_record_id"]
                        else ""
                    ),
                    str(error["type"]) if error["type"] else "",
                    str(error["severity"]) if error["severity"] else "",
                    str(error["message"]) if error["message"] else "",
                    str(error["date_created"]) if error["date_created"] else "",
                ]
                yield csv_writer.writerow(row)

        return Response(
            stream_with_context(generate_csv()),
            mimetype="text/csv",
            headers={
                "Content-Disposition": (
                    f"attachment; filename={source_id}_outstanding_issues.csv"
                ),
                "Content-Type": "text/csv",
            },
        )

    except Exception as e:
        logger.error(f"Error in download_current_record_issues_by_source: {repr(e)}")
        return "Error generating error report", 500
