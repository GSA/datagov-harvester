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

    model = escape(request.path.strip("/").split("/")[-1])

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
        return "Error with query", 400
