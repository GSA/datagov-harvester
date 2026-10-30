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
from database.interface import InvalidOrderByException
from harvester.utils.general_utils import convert_to_int, is_it_true

from . import api

SHARED_QUERY_PARAMS = ["facets", "page", "per_page", "paginate", "count", "order_by"]

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
    Validate that all query parameters are valid for the given endpoint.

    Args:
        model: The endpoint model name (e.g., "harvest_job_errors")
        provided_params: Dict of query parameters provided in request

    Returns:
        Tuple of (is_valid, error_message). error_message is empty string if valid.
    """
    valid_filters = ENDPOINT_VALID_FILTERS.get(model, [])
    allowed_params = set(SHARED_QUERY_PARAMS + valid_filters)

    for param in provided_params:
        if param == "severity":
            continue
        if param not in allowed_params:
            valid_filters_str = ", ".join(valid_filters) if valid_filters else "none"
            return False, (
                f"{param} is not a valid parameter for /{model}/. "
                f"Valid filter parameters: {valid_filters_str}. "
                f"Valid shared parameters: {', '.join(SHARED_QUERY_PARAMS)}."
            )

    return True, ""


def _json_builder_query_impl(**kwargs):
    model = escape(request.path.strip("/").split("/")[-1])

    is_valid, error_msg = _validate_query_parameters(model, request.args.keys())
    if not is_valid:
        return jsonify({"error": error_msg}), 400

    job_id = request.args.get("harvest_job_id")
    source_id = request.args.get("harvest_source_id")
    record_id = request.args.get("harvest_record_id")
    org_id = request.args.get("organization_id")
    facets = request.args.get("facets", default="")

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
    if org_id is not None:
        if facets:
            facets += f",organization_id eq {org_id}"
        else:
            facets = f"organization_id eq {org_id}"

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

            from app.deps import build_collection_response, get_model_class_from_name
            from database.interface import (
                PAGINATE_ENTRIES_PER_PAGE,
                resolve_order_by_string,
            )

            if paginate_flag is False:
                actual_page = 0
                actual_per_page = total
            else:
                actual_per_page = (
                    PAGINATE_ENTRIES_PER_PAGE if per_page is None else per_page
                )
                actual_per_page = max(min(actual_per_page, 100), 1)
                actual_page = 0 if page is None else page
                actual_page = max(actual_page, 0)

            model_class = get_model_class_from_name(model)
            resolved_order_by = resolve_order_by_string(model_class, order_by)

            response = build_collection_response(
                data=deps.db._to_dict(res),
                path=request.path,
                query_string=request.query_string.decode(),
                page=actual_page,
                per_page=actual_per_page,
                total=total,
                order_by=resolved_order_by,
            )

            return jsonify(response)
        else:
            return f"No {model} found for this query", 404
    except InvalidPaginationException as e:
        logger.info(f"Invalid pagination in json_builder_query :: {repr(e)}")
        return JSON_INVALID_PAGINATION(str(e))
    except InvalidOrderByException as e:
        logger.info(f"Invalid order_by in json_builder_query :: {repr(e)}")
        return jsonify({"error": "Invalid order_by parameter"}), 422
    except Exception as e:
        logger.info(f"Failed json_builder_query :: {repr(e)} ")
        return jsonify({"error": "Error with query"}), 400


@api.route("/organizations/", methods=["GET"])
@api.input(OrganizationsQueryInfo, location="query", validation=False)
@api.doc(
    responses={
        200: {
            "description": "Structured collection response with data, meta, and links",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "data": {
                                "type": "array",
                                "items": OrgInfo,
                            },
                            "meta": {
                                "type": "object",
                                "properties": {
                                    "page": {"type": "integer"},
                                    "per_page": {"type": "integer"},
                                    "total": {"type": "integer"},
                                    "page_count": {"type": "integer"},
                                    "order_by": {"type": "string"},
                                    "api_version": {"type": "string"},
                                    "application_version": {"type": "string"},
                                },
                            },
                            "links": {
                                "type": "object",
                                "properties": {
                                    "self": {"type": "string"},
                                    "next": {"type": ["string", "null"]},
                                    "previous": {"type": ["string", "null"]},
                                    "documentation": {"type": "string"},
                                    "changelog": {"type": ["string", "null"]},
                                },
                            },
                        },
                        "required": ["data", "meta", "links"],
                    }
                },
            },
        }
    }
)
def organizations_query(**kwargs):
    return _json_builder_query_impl(**kwargs)


@api.route("/harvest_sources/", methods=["GET"])
@api.input(HarvestSourcesQueryInfo, location="query", validation=False)
@api.doc(
    responses={
        200: {
            "description": "Structured collection response with data, meta, and links",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "data": {
                                "type": "array",
                                "items": SourceInfo,
                            },
                            "meta": {
                                "type": "object",
                                "properties": {
                                    "page": {"type": "integer"},
                                    "per_page": {"type": "integer"},
                                    "total": {"type": "integer"},
                                    "page_count": {"type": "integer"},
                                    "order_by": {"type": "string"},
                                    "api_version": {"type": "string"},
                                    "application_version": {"type": "string"},
                                },
                            },
                            "links": {
                                "type": "object",
                                "properties": {
                                    "self": {"type": "string"},
                                    "next": {"type": ["string", "null"]},
                                    "previous": {"type": ["string", "null"]},
                                    "documentation": {"type": "string"},
                                    "changelog": {"type": ["string", "null"]},
                                },
                            },
                        },
                        "required": ["data", "meta", "links"],
                    }
                },
            },
        }
    }
)
def harvest_sources_query(**kwargs):
    return _json_builder_query_impl(**kwargs)


@api.route("/harvest_jobs/", methods=["GET"])
@api.input(HarvestJobsQueryInfo, location="query", validation=False)
@api.doc(
    responses={
        200: {
            "description": "Structured collection response with data, meta, and links",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "data": {
                                "type": "array",
                                "items": JobInfo,
                            },
                            "meta": {
                                "type": "object",
                                "properties": {
                                    "page": {"type": "integer"},
                                    "per_page": {"type": "integer"},
                                    "total": {"type": "integer"},
                                    "page_count": {"type": "integer"},
                                    "order_by": {"type": "string"},
                                    "api_version": {"type": "string"},
                                    "application_version": {"type": "string"},
                                },
                            },
                            "links": {
                                "type": "object",
                                "properties": {
                                    "self": {"type": "string"},
                                    "next": {"type": ["string", "null"]},
                                    "previous": {"type": ["string", "null"]},
                                    "documentation": {"type": "string"},
                                    "changelog": {"type": ["string", "null"]},
                                },
                            },
                        },
                        "required": ["data", "meta", "links"],
                    }
                },
            },
        }
    }
)
def harvest_jobs_query(**kwargs):
    return _json_builder_query_impl(**kwargs)


@api.route("/harvest_records/", methods=["GET"])
@api.input(HarvestRecordsQueryInfo, location="query", validation=False)
@api.doc(
    responses={
        200: {
            "description": "Structured collection response with data, meta, and links",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "data": {
                                "type": "array",
                                "items": RecordInfo,
                            },
                            "meta": {
                                "type": "object",
                                "properties": {
                                    "page": {"type": "integer"},
                                    "per_page": {"type": "integer"},
                                    "total": {"type": "integer"},
                                    "page_count": {"type": "integer"},
                                    "order_by": {"type": "string"},
                                    "api_version": {"type": "string"},
                                    "application_version": {"type": "string"},
                                },
                            },
                            "links": {
                                "type": "object",
                                "properties": {
                                    "self": {"type": "string"},
                                    "next": {"type": ["string", "null"]},
                                    "previous": {"type": ["string", "null"]},
                                    "documentation": {"type": "string"},
                                    "changelog": {"type": ["string", "null"]},
                                },
                            },
                        },
                        "required": ["data", "meta", "links"],
                    }
                },
            },
        }
    }
)
def harvest_records_query(**kwargs):
    return _json_builder_query_impl(**kwargs)


@api.route("/harvest_job_errors/", methods=["GET"])
@api.input(HarvestJobErrorsQueryInfo, location="query", validation=False)
@api.doc(
    responses={
        200: {
            "description": "Structured collection response with data, meta, and links",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "data": {
                                "type": "array",
                                "items": ErrorInfo,
                            },
                            "meta": {
                                "type": "object",
                                "properties": {
                                    "page": {"type": "integer"},
                                    "per_page": {"type": "integer"},
                                    "total": {"type": "integer"},
                                    "page_count": {"type": "integer"},
                                    "order_by": {"type": "string"},
                                    "api_version": {"type": "string"},
                                    "application_version": {"type": "string"},
                                },
                            },
                            "links": {
                                "type": "object",
                                "properties": {
                                    "self": {"type": "string"},
                                    "next": {"type": ["string", "null"]},
                                    "previous": {"type": ["string", "null"]},
                                    "documentation": {"type": "string"},
                                    "changelog": {"type": ["string", "null"]},
                                },
                            },
                        },
                        "required": ["data", "meta", "links"],
                    }
                },
            },
        }
    }
)
def harvest_job_errors_query(**kwargs):
    return _json_builder_query_impl(**kwargs)


@api.route("/harvest_record_errors/", methods=["GET"])
@api.input(HarvestRecordErrorsQueryInfo, location="query", validation=False)
@api.doc(
    responses={
        200: {
            "description": "Structured collection response with data, meta, and links",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "data": {
                                "type": "array",
                                "items": ErrorInfo,
                            },
                            "meta": {
                                "type": "object",
                                "properties": {
                                    "page": {"type": "integer"},
                                    "per_page": {"type": "integer"},
                                    "total": {"type": "integer"},
                                    "page_count": {"type": "integer"},
                                    "order_by": {"type": "string"},
                                    "api_version": {"type": "string"},
                                    "application_version": {"type": "string"},
                                },
                            },
                            "links": {
                                "type": "object",
                                "properties": {
                                    "self": {"type": "string"},
                                    "next": {"type": ["string", "null"]},
                                    "previous": {"type": ["string", "null"]},
                                    "documentation": {"type": "string"},
                                    "changelog": {"type": ["string", "null"]},
                                },
                            },
                        },
                        "required": ["data", "meta", "links"],
                    }
                },
            },
        }
    }
)
def harvest_record_errors_query(**kwargs):
    return _json_builder_query_impl(**kwargs)
