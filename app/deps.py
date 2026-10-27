import logging
import os
from functools import cache, wraps

from dotenv import load_dotenv
from flask import Response, current_app, g, jsonify, request, session

from app.auth import LoginRequiredAuth
from database.interface import HarvesterDBInterface
from harvester.lib.load_manager import LoadManager
from harvester.utils.general_utils import is_valid_uuid4
from shared.constants import SEVERITY_VALUES

logger = logging.getLogger("harvest_admin")

load_dotenv()

db = HarvesterDBInterface()
load_manager = LoadManager()

auth = LoginRequiredAuth()
login_required = auth.login_required()

CKAN_URL = os.getenv("CKAN_URL") or ""
CATALOG_BASE_URL = os.getenv("CATALOG_BASE_URL") or ""

STATUS_STRINGS_ENUM = {404: "Not Found"}


def JSON_NOT_FOUND():
    """Return our most generic error response.

    This is a function rather than a constant because `jsonify` requires
    an app context.
    """
    response = jsonify({"error": STATUS_STRINGS_ENUM[404]})
    response.status_code = 404
    return response


class InvalidSeverityError(ValueError):
    """Raised when a request asks for a severity outside SEVERITY_VALUES."""


def JSON_INVALID_SEVERITY():
    """Return a generic 400 for invalid severity values."""
    response = jsonify(
        {"error": f"Invalid severity. Must be one of: {', '.join(SEVERITY_VALUES)}."}
    )
    response.status_code = 400
    return response


class InvalidPaginationException(Exception):
    """Raised when pagination parameters are invalid."""


def JSON_INVALID_PAGINATION(message="Invalid pagination parameter"):
    """Return a 422 JSON for invalid pagination."""
    response = jsonify({"error": message})
    response.status_code = 422
    return response


def get_requested_severity(default=None):
    """Read and validate the `severity` query param.

    Returns `default` when the param is absent — None, meaning "don't filter",
    since the API reads return every issue and let callers tell them apart by
    the `severity` field. Raises InvalidSeverityError for anything outside
    SEVERITY_VALUES, rather than passing unvalidated input into the
    string-built facet DSL (see query_filter_builder, which splits facets on
    spaces and commas).
    """
    severity = request.args.get("severity")
    if severity is None:
        return default
    if severity not in SEVERITY_VALUES:
        raise InvalidSeverityError(severity)
    return severity


class UnsafeTemplateEnvError(RuntimeError):
    pass


def render_block(template_name: str, block_name: str, **context) -> Response:
    """
    Render a specific block from a Jinja template, while using the Flask's
    default environment.
    """
    env = current_app.jinja_env
    if not getattr(env, "autoescape", None):
        raise UnsafeTemplateEnvError(
            "Jinja autoescape is disabled; enable it or use Flask's jinja_env."
        )
    template = env.get_template(template_name)

    block_gen = template.blocks[block_name]
    html = "".join(block_gen(template.new_context(context)))
    return Response(html, mimetype="text/html; charset=utf-8")


def valid_id_required(f):
    """Decorator to check that ALL function arguments are valid IDs.

    Returns a JSON 404 if they are not.

    TODO: Make the decorator return a pretty templated HTML page if the user
    would be expecting a 404 page.
    """

    @wraps(f)
    def decorated_function(*args, **kwargs):
        for arg in args:
            if not is_valid_uuid4(arg):
                return JSON_NOT_FOUND()
        for kwarg in kwargs.values():
            if not is_valid_uuid4(kwarg):
                return JSON_NOT_FOUND()
        return f(*args, **kwargs)

    return decorated_function


def _get_org_by_identifier(org_identifier: str):
    """Resolve an organization by UUID id, slug, or alias."""
    org = None
    if is_valid_uuid4(org_identifier):
        org = db.get_organization(org_identifier)
    if org is None:
        org = db.get_organization_by_slug(org_identifier)
    if org is None:
        org = db.get_organization_by_alias(org_identifier)
    return org


def _get_org_url_identifier(org) -> str | None:
    """Prefer an organization's slug for URLs, with UUID as a fallback."""
    if org is None:
        return None
    return org.slug or org.id


def _get_request_actor() -> str:
    actor = getattr(g, "request_actor", None) or session.get("user")
    return actor or "<anonymous>"


def _get_request_auth_type() -> str:
    auth_type = getattr(g, "request_auth_type", None)
    if auth_type:
        return auth_type
    if session.get("user"):
        return "session"
    return "anonymous"


def _log_mutation(action: str, entity: str, entity_id: str | None = None, **details):
    detail_fields = {
        "user": _get_request_actor(),
        "auth_type": _get_request_auth_type(),
        "method": request.method,
        "path": request.path,
    }
    if entity_id is not None:
        detail_fields[f"{entity}_id"] = entity_id
    detail_fields.update(details)
    ordered_keys = sorted(detail_fields)
    message = "Audit %s %s " % (action, entity) + " ".join(
        f"{key}=%s" for key in ordered_keys
    )
    logger.info(message, *(detail_fields[key] for key in ordered_keys))


def create_harvest_source(source_data):
    existing = db.get_harvest_source_by_url(source_data.get("url"))
    if existing:
        message = (
            f"A harvest source with this URL already exists "
            f"(source ID: {existing.id}). "
            "Use a different URL or edit the existing source."
        )
        return None, message, 409

    source, error = db.try_add_harvest_source(source_data)
    if not source:
        return None, error or "Failed to add harvest source.", 400

    job_message = load_manager.reschedule_next_run(source.id)
    if not job_message:
        return source, "Failed to schedule the first harvest job.", 500

    _log_mutation(
        "create",
        "harvest_source",
        source.id,
        organization_id=source.organization_id,
        source_name=source.name,
    )
    return source, f"Added new harvest source with ID: {source.id}. {job_message}", 200


@cache
def get_application_version():
    """
    Get application version from environment or git.

    Cached after first call since the version cannot change while the process runs.

    Checks in order:
    1. APP_VERSION env var (commit SHA set during deploy)
    2. git rev-parse (local development)

    Returns:
        Version string (e.g., "a1b2c3d") or "unknown"
    """
    import os
    import subprocess

    # Check for APP_VERSION env var (set in manifest from --var app_version during deploy)
    app_version = os.environ.get("APP_VERSION")
    if app_version:
        return app_version

    # Fall back to git for local development
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short=7", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        return result.stdout.strip()
    except Exception:
        pass

    return "unknown"


def get_api_version_from_path(path):
    """
    Extract API version from request path.

    Args:
        path: Request path (e.g., "/api/v1/harvest_jobs/")

    Returns:
        API version string (e.g., "v1")
    """
    from app.routes import API_VERSIONS

    parts = path.strip("/").split("/")
    if len(parts) >= 2 and parts[0] == "api":
        version_part = parts[1]
        api_versions = {version for version, _ in API_VERSIONS}
        if version_part in api_versions:
            return version_part

    latest_version, _ = API_VERSIONS[-1]
    return latest_version


def get_model_class_from_name(model_name):
    """Get the model class from the model name string."""
    from database.models import (
        HarvestJob,
        HarvestJobError,
        HarvestRecord,
        HarvestRecordError,
        HarvestSource,
        Organization,
    )

    model_map = {
        "organizations": Organization,
        "harvest_sources": HarvestSource,
        "harvest_records": HarvestRecord,
        "harvest_jobs": HarvestJob,
        "harvest_job_errors": HarvestJobError,
        "harvest_record_errors": HarvestRecordError,
    }
    return model_map.get(model_name)


def build_collection_response(
    data, path, query_string, page, per_page, total, order_by
):
    """
    Build structured collection response with data, meta, and links.

    Args:
        data: List of result records
        path: Request path (e.g., "/api/v1/harvest_jobs/")
        query_string: Request query string (e.g., "page=0&per_page=10")
        page: Current page number (0-indexed)
        per_page: Page size
        total: Total record count
        order_by: Applied ordering (e.g., "-date_created")

    Returns:
        Dict with data, meta, and links structure
    """
    import math
    from urllib.parse import parse_qs, urlencode

    page_count = math.ceil(total / per_page) if per_page > 0 else 0

    api_version = get_api_version_from_path(path)
    app_version = get_application_version()

    query_params = parse_qs(query_string) if query_string else {}
    base_params = {
        k: v[0] if isinstance(v, list) else v for k, v in query_params.items()
    }
    base_params["page"] = page
    base_params["per_page"] = per_page
    if order_by and order_by not in ("asc", "desc"):
        base_params["order_by"] = order_by

    base_path = path
    if not path.startswith("/api/"):
        base_path = f"/api/{api_version}{path}"

    def build_url(page_num):
        params = base_params.copy()
        params["page"] = page_num
        return f"{base_path}?{urlencode(params)}"

    meta = {
        "page": page,
        "per_page": per_page,
        "total": total,
        "page_count": page_count,
        "order_by": order_by,
        "api_version": api_version,
        "application_version": app_version,
    }

    links = {
        "self": build_url(page),
        "next": build_url(page + 1) if page < page_count - 1 else None,
        "previous": build_url(page - 1) if page > 0 else None,
        "documentation": "/openapi/docs",
        "changelog": None,  # TODO: Set to "/api/changelog" once #6364/#6369 is implemented
    }

    return {"data": data, "meta": meta, "links": links}
