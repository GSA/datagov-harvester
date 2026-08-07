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
    JSON_INVALID_SEVERITY,
    InvalidSeverityError,
    get_requested_severity,
    logger,
)
from harvester.utils.general_utils import convert_to_int, is_it_true

from . import api


def _facets_on_severity(facets: str) -> bool:
    """Report whether a facet string already constrains `severity`.

    Parses the same way query_filter_builder does — comma-separated
    "column op value" triples — so this agrees with what actually reaches
    the query. Malformed facets are left for the interface layer to reject.
    """
    for facet in facets.split(","):
        parts = facet.split(maxsplit=2)
        if parts and parts[0] == "severity":
            return True
    return False


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

    # severity only exists on harvest_record_error, so it is ignored elsewhere.
    # Defaults to "error" to match every other record-error read path, so this
    # endpoint doesn't return warnings under a name that promises errors.
    #
    # A caller who already filtered on severity via ?facets= has said what they
    # want. Injecting the default there would AND a second, contradictory
    # severity condition onto their query and return nothing.
    if model == "harvest_record_errors" and not _facets_on_severity(facets):
        try:
            severity = get_requested_severity()
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
        if not res:
            return f"No {model} found for this query", 404
        elif isinstance(res, int):
            return {"count": res, "type": model}
        else:
            return jsonify(deps.db._to_dict(res))
    except Exception as e:
        logger.info(f"Failed json_builder_query :: {repr(e)} ")
        return "Error with query", 400
