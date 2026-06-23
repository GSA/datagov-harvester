import json

from flask import Response, jsonify, make_response, request

from app import deps
from app.api_schemas import ErrorInfo, RecordInfo
from app.deps import (
    JSON_NOT_FOUND,
    _log_mutation,
    logger,
    login_required,
    valid_id_required,
)

from . import api


@api.route("/harvest_record/<record_id>", methods=["GET"])
@api.output(RecordInfo)
@valid_id_required
def get_harvest_record(record_id):
    record = deps.db.get_harvest_record(record_id)
    return jsonify(deps.db._to_dict(record)) if record else JSON_NOT_FOUND()


@api.route("/harvest_record/<record_id>/raw", methods=["GET"])
@api.doc(
    responses={
        200: {
            "description": "Raw harvest record contents",
            "content": {
                "application/json": {"type": "object"},
                "application/xml": {"type": "object"},
            },
        }
    }
)
@valid_id_required
def get_harvest_record_raw(record_id=None):
    record = deps.db.get_harvest_record(record_id)
    if not record or not record.job or not record.job.source:
        return JSON_NOT_FOUND()

    source = record.job.source
    schema_type = getattr(source, "schema_type", None)

    if schema_type and "dcatus" in schema_type:
        try:
            source_raw_json = json.loads(record.source_raw)
            return jsonify(source_raw_json), 200
        except Exception:
            logger.error(f"Error returning JSON source raw for record ID: {record_id}")
            return (
                Response(record.source_raw, mimetype="application/json; charset=utf-8"),
                200,
            )
    else:
        return (
            Response(record.source_raw, mimetype="application/xml; charset=utf-8"),
            200,
        )


@api.route("/harvest_record/<record_id>/transformed", methods=["GET"])
@api.doc(
    responses={
        200: {
            "description": "Transformed harvest record contents",
            "content": {
                "application/json": {"type": "object"},
            },
        }
    }
)
@valid_id_required
def get_harvest_record_transformed(record_id=None):
    record = deps.db.get_harvest_record(record_id)
    if not record:
        return JSON_NOT_FOUND()

    transformed = record.source_transform
    if transformed is None:
        return JSON_NOT_FOUND()

    if isinstance(transformed, str):
        if not transformed.strip():
            return JSON_NOT_FOUND()
        try:
            transformed = json.loads(transformed)
        except json.JSONDecodeError:
            return (
                Response(transformed, mimetype="application/json; charset=utf-8"),
                200,
            )

    return jsonify(transformed), 200


@api.post("/harvest_record/add")
@api.doc(hide=True)
@login_required
def add_harvest_record():
    if not request.is_json:
        return make_response(jsonify({"error": "Request must be JSON"}), 415)
    record = deps.db.add_harvest_record(request.json)
    if record:
        _log_mutation(
            "create",
            "harvest_record",
            record.id,
            harvest_job_id=record.harvest_job_id,
        )
        return {"message": f"Added new record with ID: {record.id}"}, 200
    else:
        return {"error": "Failed to add harvest record."}, 400


@api.route("/harvest_record/<record_id>/errors", methods=["GET"])
@api.doc(
    responses={
        200: {
            "description": "List of errors for this record",
            "content": {
                "application/json": {"schema": {"type": "array", "items": ErrorInfo}}
            },
        },
    }
)
@valid_id_required
def get_all_harvest_record_errors(record_id: str) -> list:
    try:
        record_errors = deps.db.get_harvest_record_errors_by_record(record_id)
        return (
            jsonify(deps.db._to_dict(record_errors))
            if record_errors
            else JSON_NOT_FOUND()
        )
    except Exception:
        return jsonify({"error": "Please provide a valid record_id"}), 404


@api.route("/harvest_error/<error_id>", methods=["GET"])
@api.output(ErrorInfo)
@valid_id_required
def get_harvest_error(error_id: str = None) -> Response:
    try:
        error = deps.db.get_harvest_error(error_id)
        return jsonify(deps.db._to_dict(error)) if error else JSON_NOT_FOUND()
    except Exception:
        return jsonify({"error": "Please provide a valid record_id"}), 404
