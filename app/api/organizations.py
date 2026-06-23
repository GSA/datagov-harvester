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

    org = deps.db.add_organization(org_data)
    if org:
        _log_mutation("create", "organization", org.id, organization_slug=org.slug)
        return make_response(
            jsonify({"message": f"Added new organization with ID: {org.id}"}), 200
        )
    else:
        return make_response(jsonify({"error": "Failed to add organization."}), 400)


@api.post("/organization/edit/<string:org_id>")
@api.doc(hide=True)
@login_required
@valid_id_required
def edit_organization_api(org_id):
    org = deps.db.update_organization(org_id, request.json)
    if org:
        _log_mutation("edit", "organization", org.id, organization_slug=org.slug)
        return {"message": f"Updated org with ID: {org.id}"}, 200
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
    """Return the JSON representation of an organization by UUID or slug."""
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
