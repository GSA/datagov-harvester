import csv
import json
import os
import tempfile

from apiflask.schemas import FileSchema
from flask import Response, flash, jsonify, make_response, redirect, request, url_for
from markupsafe import escape

from app.api_schemas import JobInfo
from app.blueprints import deps
from app.blueprints.deps import (
    JSON_NOT_FOUND,
    _log_mutation,
    login_required,
    logger,
    valid_id_required,
)
from harvester.utils.general_utils import is_valid_uuid4

from . import api


@api.post("/api/harvest_job/add")
@api.doc(hide=True)
@login_required
def add_harvest_job():
    if not request.is_json:
        return make_response(jsonify({"error": "Request must be JSON"}), 415)
    job = deps.db.add_harvest_job(request.json)
    if job:
        _log_mutation(
            "create",
            "harvest_job",
            job.id,
            harvest_source_id=job.harvest_source_id,
            status=job.status,
        )
        return {"message": f"Added new harvest job with ID: {job.id}"}, 200
    else:
        return {"error": "Failed to add harvest job."}, 400


@api.get("/api/harvest_job/<job_id>")
@api.doc(
    responses={
        200: {
            "description": "View harvest job",
            "content": {"application/json": {"schema": JobInfo}},
        }
    }
)
@valid_id_required
def get_harvest_job(job_id):
    """Return the JSON representation of a harvest job."""
    job = deps.db.get_harvest_job(job_id)
    return jsonify(deps.db._to_dict(job)) if job else JSON_NOT_FOUND()


@api.route("/harvest_job/<job_id>", methods=["PUT"])
@api.doc(hide=True)
@login_required
@valid_id_required
def update_harvest_job(job_id):
    result = deps.db.update_harvest_job(job_id, request.json)
    _log_mutation(
        "edit",
        "harvest_job",
        job_id,
        harvest_source_id=getattr(result, "harvest_source_id", None),
        status=getattr(result, "status", None),
    )
    return jsonify(deps.db._to_dict(result))


@api.route("/harvest_job/<job_id>", methods=["DELETE"])
@api.doc(hide=True)
@login_required
@valid_id_required
def delete_harvest_job(job_id):
    result = deps.db.delete_harvest_job(job_id)
    _log_mutation("delete", "harvest_job", job_id)
    return escape(result)


@api.route("/harvest_job/cancel/<job_id>", methods=["GET", "POST"])
@api.doc(hide=True)
@login_required
@valid_id_required
def cancel_harvest_job(job_id):
    """Cancels a harvest job"""
    if not is_valid_uuid4(job_id):
        flash("Invalid job ID.")
        return redirect("/")
    message = deps.load_manager.stop_job(job_id)
    flash(message)
    return redirect(url_for("main.view_harvest_job", job_id=job_id))


@api.route("/harvest_job/<string:job_id>/errors/<string:error_type>", methods=["GET"])
@api.output(FileSchema, content_type="text/csv")
def download_harvest_errors_by_job(job_id, error_type):
    if not is_valid_uuid4(job_id):
        return JSON_NOT_FOUND()
    try:
        if error_type not in ["job", "record"]:
            return "Invalid error type. Must be 'job' or 'record'", 400

        temp_file = tempfile.NamedTemporaryFile(mode="w+", delete=False, suffix=".csv")
        temp_file_path = temp_file.name

        try:
            csv_writer = csv.writer(temp_file)

            if error_type == "job":
                header = [
                    "harvest_job_id",
                    "date_created",
                    "job_error_type",
                    "message",
                    "harvest_job_error_id",
                ]
                csv_writer.writerow(header)

                errors = deps.db._to_dict(deps.db.get_harvest_job_errors_by_job(job_id))
                for error_dict in errors:
                    row = [
                        str(error_dict.get("harvest_job_id", "")),
                        str(error_dict.get("date_created", "")),
                        str(error_dict.get("type", "")),
                        str(error_dict.get("message", "")),
                        str(error_dict.get("id", "")),
                    ]
                    csv_writer.writerow(row)

            elif error_type == "record":
                header = [
                    "record_error_id",
                    "identifier",
                    "title",
                    "harvest_record_id",
                    "record_error_type",
                    "message",
                    "date_created",
                ]
                csv_writer.writerow(header)

                page = 0
                batch_size = 100

                while True:
                    batch_errors = deps.db.get_harvest_record_errors_by_job(
                        job_id,
                        paginate=True,
                        page=page,
                        per_page=batch_size,
                    )

                    if not batch_errors:
                        break

                    for error, identifier, source_raw in batch_errors:
                        title = ""
                        if source_raw:
                            try:
                                title = json.loads(source_raw).get("title", "")
                            except (json.JSONDecodeError, AttributeError):
                                title = ""

                        row = [
                            str(error.id) if error.id else "",
                            str(identifier) if identifier else "",
                            str(title),
                            (
                                str(error.harvest_record_id)
                                if error.harvest_record_id
                                else ""
                            ),
                            str(error.type) if error.type else "",
                            str(error.message) if error.message else "",
                            str(error.date_created) if error.date_created else "",
                        ]
                        csv_writer.writerow(row)

                    page += 1

                    if len(batch_errors) < batch_size:
                        break

            temp_file.close()

            def generate_file_chunks():
                try:
                    with open(temp_file_path, "r") as f:
                        while True:
                            chunk = f.read(8192)
                            if not chunk:
                                break
                            yield chunk
                finally:
                    try:
                        os.unlink(temp_file_path)
                    except OSError:
                        pass

            response = Response(
                generate_file_chunks(),
                mimetype="text/csv",
                headers={
                    "Content-Disposition": f"attachment; filename={job_id}_{error_type}_errors.csv",
                    "Content-Type": "text/csv",
                },
            )

            return response

        except Exception as e:
            temp_file.close()
            try:
                os.unlink(temp_file_path)
            except OSError:
                pass
            raise e

    except Exception as e:
        logger.error(f"Error in download_harvest_errors_by_job: {repr(e)}")
        return "Error generating error report", 500
