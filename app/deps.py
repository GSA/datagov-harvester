import logging
import os
from functools import wraps

from dotenv import load_dotenv
from flask import Response, current_app, g, jsonify, request, session

from app.auth import LoginRequiredAuth
from database.interface import HarvesterDBInterface
from harvester.lib.load_manager import LoadManager
from harvester.utils.general_utils import is_valid_uuid4

logger = logging.getLogger("harvest_admin")

load_dotenv()

db = HarvesterDBInterface()
load_manager = LoadManager()

auth = LoginRequiredAuth()
login_required = auth.login_required()

CKAN_URL = os.getenv("CKAN_URL") or ""

STATUS_STRINGS_ENUM = {404: "Not Found"}


def JSON_NOT_FOUND():
    """Return our most generic error response.

    This is a function rather than a constant because `jsonify` requires
    an app context.
    """
    response = jsonify({"error": STATUS_STRINGS_ENUM[404]})
    response.status_code = 404
    return response


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

    job_message = load_manager.schedule_first_job(source.id)
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
