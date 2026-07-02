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
from app.deps import logger
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

    # Object type is the last path segment: "/api/harvest_jobs/" -> "harvest_jobs".
    model = escape(request.path.strip("/").split("/")[-1])
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
        if not res:
            return f"No {model} found for this query", 404
        elif isinstance(res, int):
            return {"count": res, "type": model}
        else:
            return jsonify(deps.db._to_dict(res))
    except Exception as e:
        logger.info(f"Failed json_builder_query :: {repr(e)} ")
        return "Error with query", 400
