from flask import jsonify, request
from markupsafe import escape

from app import deps
from app.api_schemas import (
    ErrorInfo,
    HarvestJobErrorsQueryInfo,
    HarvestJobsQueryInfo,
    HarvestRecordErrorsQueryInfo,
    HarvestRecordsQueryInfo,
    HarvestSourcesQueryInfo,
    JobInfo,
    OrganizationsQueryInfo,
    OrgInfo,
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


def _json_builder_query_impl(**kwargs):
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

    # severity only exists on harvest_record_error, so it is ignored elsewhere.
    # No default: an absent param returns every issue, matching the job-level
    # reads (#799). Callers tell errors from warnings by the `severity` field
    # now published on each row, and narrow with ?severity= when they want one.
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
    try:
        res = deps.db.pget_db_query(
            model=model,
            page=request.args.get("page", type=convert_to_int),
            per_page=request.args.get("per_page", type=convert_to_int),
            paginate=request.args.get("paginate", type=is_it_true),
            count=request.args.get("count", type=is_it_true),
            order_by=request.args.get("order_by"),
            facets=facets,
        )
        if isinstance(res, int):
            return {"count": res, "type": model}
        elif isinstance(res, list):
            if not res and request.args.get("per_page", type=convert_to_int) != 0:
                return f"No {model} found for this query", 404
            return jsonify(deps.db._to_dict(res))
        else:
            return f"No {model} found for this query", 404
    except InvalidPaginationException as e:
        logger.info(f"Invalid pagination in json_builder_query :: {repr(e)}")
        return JSON_INVALID_PAGINATION(str(e))
    except Exception as e:
        logger.info(f"Failed json_builder_query :: {repr(e)} ")
        return "Error with query", 400


@api.route("/organizations/", methods=["GET"])
@api.input(OrganizationsQueryInfo, location="query", validation=False)
def organizations_query(**kwargs):
    return _json_builder_query_impl(**kwargs)


@api.route("/harvest_sources/", methods=["GET"])
@api.input(HarvestSourcesQueryInfo, location="query", validation=False)
def harvest_sources_query(**kwargs):
    return _json_builder_query_impl(**kwargs)


@api.route("/harvest_jobs/", methods=["GET"])
@api.input(HarvestJobsQueryInfo, location="query", validation=False)
def harvest_jobs_query(**kwargs):
    return _json_builder_query_impl(**kwargs)


@api.route("/harvest_records/", methods=["GET"])
@api.input(HarvestRecordsQueryInfo, location="query", validation=False)
def harvest_records_query(**kwargs):
    return _json_builder_query_impl(**kwargs)


@api.route("/harvest_job_errors/", methods=["GET"])
@api.input(HarvestJobErrorsQueryInfo, location="query", validation=False)
def harvest_job_errors_query(**kwargs):
    return _json_builder_query_impl(**kwargs)


@api.route("/harvest_record_errors/", methods=["GET"])
@api.input(HarvestRecordErrorsQueryInfo, location="query", validation=False)
def harvest_record_errors_query(**kwargs):
    return _json_builder_query_impl(**kwargs)
