from flask import jsonify, make_response, request
from marshmallow import ValidationError

from app import deps
from app.api_schemas import OrgCreate, OrgInfo
from app.deps import (
    _get_org_by_identifier,
    _log_mutation,
    logger,
    login_required,
    valid_id_required,
)

from . import api


@api.get("/organization_list/")
@api.doc(
    responses={
        200: {
            "description": "List organizations",
            "content": {
                "application/json": {"schema": {"type": "array", "items": OrgInfo}}
            },
        }
    }
)
def organization_list_api():
    """Return the JSON list of all organizations."""
    return jsonify(deps.db._to_dict(deps.db.get_all_organizations()))


@api.post("/organization/add")
@api.doc(hide=True)
@api.input(OrgCreate, validation=False)
@login_required
def add_organization_api(**kwargs):
    org_data = request.json or {}
    try:
        org_data = OrgCreate().load(org_data)
    except ValidationError as e:
        return make_response(jsonify({"detail": e.messages}), 422)

    # Convert empty string to None for code_repo_url
    if "code_repo_url" in org_data:
        code_repo_url = org_data.get("code_repo_url")
        if code_repo_url:
            org_data["code_repo_url"] = code_repo_url.strip() or None
        else:
            org_data["code_repo_url"] = None

    # Check for conflict between URL and exempt flag (error, not warning)
    if org_data.get("code_repo_url") and org_data.get("code_repo_exempt"):
        return make_response(
            jsonify(
                {
                    "error": "An organization cannot have both a repository URL and "
                    "an exemption. Please provide either a URL or mark as exempt."
                }
            ),
            400,
        )

    org = deps.db.add_organization(org_data)
    if org:
        _log_mutation("create", "organization", org.id, organization_slug=org.slug)
        response_data = org.to_dict()
        return make_response(jsonify(response_data), 201)
    else:
        return make_response(jsonify({"error": "Failed to add organization."}), 400)


@api.post("/organization/edit/<string:org_id>")
@api.doc(hide=True)
@login_required
@valid_id_required
def edit_organization_api(org_id):
    org_data = request.json or {}

    # Validate code_repo_url if provided
    if "code_repo_url" in org_data:
        code_repo_url = org_data.get("code_repo_url")
        if code_repo_url and code_repo_url.strip():
            if not (
                code_repo_url.startswith("http://")
                or code_repo_url.startswith("https://")
            ):
                return make_response(
                    jsonify({"error": "URL must start with http:// or https://"}), 400
                )
            org_data["code_repo_url"] = code_repo_url.strip()
        else:
            org_data["code_repo_url"] = None

    # Get current org to check conflict
    current_org = deps.db.get_organization(org_id)
    final_code_repo_url = org_data.get(
        "code_repo_url",
        current_org.code_repo_url if current_org else None,
    )
    final_code_repo_exempt = org_data.get(
        "code_repo_exempt",
        current_org.code_repo_exempt if current_org else False,
    )

    # Check for conflict between URL and exempt flag (error, not warning)
    if final_code_repo_url and final_code_repo_exempt:
        return make_response(
            jsonify(
                {
                    "error": "An organization cannot have both a repository URL and "
                    "an exemption. Please provide either a URL or mark as exempt."
                }
            ),
            400,
        )

    org = deps.db.update_organization(org_id, org_data)
    if org:
        _log_mutation("edit", "organization", org.id, organization_slug=org.slug)
        response_data = {"message": f"Updated org with ID: {org.id}"}
        return response_data, 200
    else:
        return {"error": "Failed to update organization."}, 400


@api.get("/organization/<string:org_identifier>")
@api.doc(
    responses={
        200: {
            "description": "View organization info",
            "content": {"application/json": {"schema": OrgInfo}},
        }
    }
)
def get_organization(org_identifier: str):
    """Return the JSON representation of an organization by UUID, slug, or alias."""
    org = _get_org_by_identifier(org_identifier)
    if org is None:
        return make_response(jsonify({"message": "Organization not found"}), 404)
    return jsonify(org.to_dict())


@api.route("/organization/<string:org_id>", methods=["DELETE"])
@api.doc(hide=True)
@login_required
@valid_id_required
def delete_organization(org_id):
    try:
        message, status = deps.db.delete_organization(org_id)
        _log_mutation("delete", "organization", org_id, status=status)
        return make_response(jsonify({"message": message}), status)
    except Exception as e:
        message = f"Failed to delete organization :: {repr(e)}"
        logger.error(message)
        return make_response(jsonify({"message": message}), 500)
