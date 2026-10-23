from datetime import timezone

from flask import jsonify, request
from markupsafe import escape

from app import deps
from app.api_schemas import (
    ErrorInfo,
    JobInfo,
    OrgInfo,
    QueryInfo,
    RecordInfo,
    SourceInfo,
)
from app.deps import (
    JSON_INVALID_PAGINATION,
    JSON_INVALID_SEVERITY,
    InvalidPaginationException,
    InvalidSeverityError,
    get_requested_severity,
    logger,
)
from database.interface import InvalidOrderByException
from harvester.utils.general_utils import convert_to_int, is_it_true

from . import api

ENDPOINT_VALID_FILTERS = {
    "organizations": [],
    "harvest_sources": ["organization_id"],
    "harvest_jobs": ["harvest_source_id"],
    "harvest_records": ["harvest_source_id", "harvest_job_id"],
    "harvest_job_errors": ["harvest_job_id"],
    "harvest_record_errors": ["harvest_job_id", "harvest_record_id", "severity"],
}


def _validate_query_parameters(model: str, provided_params: dict) -> tuple[bool, str]:
    """
    Validate that query filter parameters are valid for the given endpoint model.

    Args:
        model: The endpoint model name (e.g., "harvest_job_errors")
        provided_params: Dict of query parameters provided in request

    Returns:
        Tuple of (is_valid, error_message). error_message is empty string if valid.
    """
    valid_filters = ENDPOINT_VALID_FILTERS.get(model, [])
    filter_params = ["harvest_source_id", "harvest_job_id", "harvest_record_id"]

    for param in filter_params:
        if param in provided_params and param not in valid_filters:
            valid_filters_str = ", ".join(valid_filters) if valid_filters else "facets"
            return False, (
                f"{param} is not a valid parameter for /{model}/. "
                f"Valid filter parameters: {valid_filters_str}."
            )

    return True, ""


def _serialize_job_datetimes(data):
    """Serialize harvest job datetime values as ISO 8601 UTC."""
    datetime_fields = ("date_created", "date_started", "date_finished")

    if isinstance(data, list):
        for item in data:
            _serialize_job_datetimes(item)
    elif isinstance(data, dict):
        for field in datetime_fields:
            value = data.get(field)

            if value is not None and hasattr(value, "isoformat"):
                if value.tzinfo is None:
                    value = value.replace(tzinfo=timezone.utc)
                else:
                    value = value.astimezone(timezone.utc)

                data[field] = value.isoformat(timespec="milliseconds").replace(
                    "+00:00", "Z"
                )

    return data


@api.route("/organizations/", methods=["GET"])
@api.route("/harvest_sources/", methods=["GET"])
@api.route("/harvest_records/", methods=["GET"])
@api.route("/harvest_jobs/", methods=["GET"])
@api.route("/harvest_job_errors/", methods=["GET"])
@api.route("/harvest_record_errors/", methods=["GET"])
@api.input(QueryInfo, location="query", validation=False)
@api.doc(
    responses={
        200: {
            "description": "List of reuslts for this query",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "array",
                        "items": {
                            "oneOf": [
                                ErrorInfo,
                                OrgInfo,
                                SourceInfo,
                                RecordInfo,
                                JobInfo,
                            ]
                        },
                    }
                },
            },
        }
    }
)
def json_builder_query(**kwargs):
    job_id = request.args.get("harvest_job_id")
    source_id = request.args.get("harvest_source_id")
    record_id = request.args.get("harvest_record_id")
    facets = request.args.get("facets", default="")

    model = escape(request.path.strip("/").split("/")[-1])

    provided_params = {}
    if source_id is not None:
        provided_params["harvest_source_id"] = source_id
    if job_id is not None:
        provided_params["harvest_job_id"] = job_id
    if record_id is not None:
        provided_params["harvest_record_id"] = record_id

    is_valid, error_msg = _validate_query_parameters(model, provided_params)
    if not is_valid:
        return jsonify({"error": error_msg}), 400

    if job_id is not None:
        if facets:
            facets += f",harvest_job_id eq {job_id}"
        else:
            facets = f"harvest_job_id eq {job_id}"
    if source_id is not None:
        if facets:
            facets += f",harvest_source_id eq {source_id}"
        else:
            facets = f"harvest_source_id eq {source_id}"
    if record_id is not None:
        if facets:
            facets += f",harvest_record_id eq {record_id}"
        else:
            facets = f"harvest_record_id eq {record_id}"

    if model == "harvest_record_errors":
        try:
            severity = get_requested_severity(default=None)
        except InvalidSeverityError:
            return JSON_INVALID_SEVERITY()
        if severity is not None:
            if facets:
                facets += f",severity eq {severity}"
            else:
                facets = f"severity eq {severity}"

    page = request.args.get("page", type=convert_to_int)
    per_page = request.args.get("per_page", type=convert_to_int)
    paginate_flag = request.args.get("paginate", type=is_it_true)
    count_flag = request.args.get("count", type=is_it_true)
    order_by = request.args.get("order_by")

    try:
        if count_flag:
            total = deps.db.pget_db_query(
                model=model,
                page=page,
                per_page=per_page,
                paginate=paginate_flag,
                count=True,
                order_by=order_by,
                facets=facets,
            )
            return {"count": total, "type": model}

        res = deps.db.pget_db_query(
            model=model,
            page=page,
            per_page=per_page,
            paginate=paginate_flag,
            count=False,
            order_by=order_by,
            facets=facets,
        )

        if isinstance(res, list):
            if not res and per_page != 0:
                return f"No {model} found for this query", 404

            total = deps.db.pget_db_query(
                model=model,
                page=page,
                per_page=per_page,
                paginate=paginate_flag,
                count=True,
                order_by=order_by,
                facets=facets,
            )

            data = deps.db._to_dict(res)

            if model == "harvest_jobs":
                data = _serialize_job_datetimes(data)

            from app.deps import build_collection_response

            response = build_collection_response(
                data=data,
                path=request.path,
                query_string=request.query_string.decode(),
                page=page if page is not None else 0,
                per_page=per_page if per_page is not None else 10,
                total=total,
                order_by=order_by if order_by else "asc",
            )

            return jsonify(response)
        else:
            return f"No {model} found for this query", 404
    except InvalidPaginationException as e:
        logger.info(f"Invalid pagination in json_builder_query :: {repr(e)}")
        return JSON_INVALID_PAGINATION(str(e))
    except InvalidOrderByException as e:
        logger.info(f"Invalid order_by in json_builder_query :: {repr(e)}")
        return jsonify({"error": str(e)}), 422
    except Exception as e:
        logger.info(f"Failed json_builder_query :: {repr(e)} ")
        return "Error with query", 400
