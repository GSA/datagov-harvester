from flask import flash, redirect, render_template, session, url_for

from app import deps
from app.deps import (
    CKAN_URL,
    _get_org_by_identifier,
    _get_org_url_identifier,
    _log_mutation,
    logger,
    login_required,
    valid_id_required,
)
from app.forms import OrganizationForm, OrganizationTriggerForm
from app.util import make_new_org_contract

from . import main


@main.route("/organization_list/", methods=["GET"])
def organization_list():
    organizations = deps.db.get_all_organizations()
    data = {"organizations": organizations}
    return render_template("view_org_list.html", data=data)


@main.route("/organization/add", methods=["GET", "POST"])
@login_required
def add_organization():
    form = OrganizationForm(db_interface=deps.db)
    if form.validate_on_submit():
        new_org = make_new_org_contract(form)
        org = deps.db.add_organization(new_org)
        if org:
            _log_mutation("create", "organization", org.id, organization_slug=org.slug)
            flash(f"Added new organization with ID: {org.id}")
        else:
            flash("Failed to add organization.")
        return redirect(url_for("main.organization_list"))
    elif form.errors:
        flash(form.errors)
        return redirect(url_for("main.add_organization"))
    return render_template(
        "edit_data.html",
        form=form,
        action="Add",
        data_type="Organization",
        button="Submit",
    )


@main.get("/organization/<string:org_identifier>")
def view_organization(org_identifier: str):
    """Render the HTML organization detail page by UUID or slug."""
    org = _get_org_by_identifier(org_identifier)
    org_id = org.id if org is not None else org_identifier

    form = OrganizationTriggerForm() if session.get("user") else None
    sources = deps.db.get_harvest_source_by_org(org_id)
    future_harvest_jobs = {}
    for source in sources:
        job = deps.db.get_new_harvest_jobs_by_source_in_future(source.id)
        if len(job):
            future_harvest_jobs[source.id] = job[0].date_created
    harvest_jobs = {}
    for source in sources:
        job = deps.db.get_first_harvest_job_by_filter(
            {"harvest_source_id": source.id, "status": "complete"}
        )
        if job:
            harvest_jobs[source.id] = job
    data = {
        "ckan_url": CKAN_URL,
        "organization": org,
        "organization_dict": deps.db._to_dict(org),
        "harvest_sources": sources,
        "harvest_jobs": harvest_jobs,
        "future_harvest_jobs": future_harvest_jobs,
    }
    return render_template(
        "view_org_data.html",
        data=data,
        form=form,
    ), (200 if org is not None else 404)


@main.post("/organization/<string:org_identifier>")
@login_required
def update_organization_actions(org_identifier: str):
    """Handle authenticated organization action form submissions."""
    org = _get_org_by_identifier(org_identifier)
    org_id = org.id if org is not None else org_identifier
    org_url_identifier = _get_org_url_identifier(org) or org_identifier
    form = OrganizationTriggerForm()

    if not form.validate_on_submit():
        flash(form.errors)
        return redirect(
            url_for("main.view_organization", org_identifier=org_url_identifier)
        )

    if form.edit.data:
        return redirect(url_for("main.edit_organization", org_id=org_id))
    elif form.delete.data:
        try:
            message, status = deps.db.delete_organization(org_id)
            _log_mutation(
                "delete",
                "organization",
                org_id,
                organization_slug=org_url_identifier,
                status=status,
            )
            flash(message)
            if status == 409:
                return redirect(
                    url_for(
                        "main.view_organization",
                        org_identifier=org_url_identifier,
                    )
                )
            else:
                return redirect(url_for("main.organization_list"))
        except Exception as e:
            message = f"Failed to delete organization :: {repr(e)}"
            logger.error(message)
            flash(message)
            return redirect(
                url_for(
                    "main.view_organization",
                    org_identifier=org_url_identifier,
                )
            )

    return redirect(
        url_for("main.view_organization", org_identifier=org_url_identifier)
    )


@main.route("/organization/edit/<string:org_id>", methods=["GET", "POST"])
@login_required
@valid_id_required
def edit_organization(org_id):
    org = deps.db._to_dict(deps.db.get_organization(org_id))
    form = OrganizationForm(organization_id=org_id, data=org, db_interface=deps.db)
    if form.validate_on_submit():
        new_org_data = make_new_org_contract(form)
        org = deps.db.update_organization(org_id, new_org_data)
        if org:
            _log_mutation("edit", "organization", org.id, organization_slug=org.slug)
            flash(f"Updated org with ID: {org.id}")
        else:
            flash("Failed to update organization.")
        return redirect(
            url_for(
                "main.view_organization",
                org_identifier=_get_org_url_identifier(org) or org_id,
            )
        )
    elif form.errors:
        flash(form.errors)
        return redirect(url_for("main.edit_organization", org_id=org_id))

    return render_template(
        "edit_data.html",
        form=form,
        action="Edit",
        data_type="Organization",
        button="Update",
    )
