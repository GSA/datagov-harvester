from flask import flash, redirect, render_template, request, url_for

from app import deps, htmx
from app.deps import (
    CKAN_URL,
    _log_mutation,
    logger,
    login_required,
    render_block,
    valid_id_required,
)
from app.forms import HarvestSourceForm, HarvestTriggerForm
from app.paginate import Pagination
from app.util import make_new_source_contract
from harvester.utils.general_utils import (
    convert_to_int,
    dynamic_map_list_items_to_dict,
    get_datetime,
)

from . import main


def trigger_manual_job_helper(source_id, job_type="harvest"):
    message = deps.load_manager.trigger_manual_job(source_id, job_type)
    flash(message)
    return redirect(url_for("main.view_harvest_source", source_id=source_id))


@main.route("/harvest_source/add", methods=["POST", "GET"])
@login_required
def add_harvest_source():
    form = HarvestSourceForm()
    organizations = deps.db.get_all_organizations()
    organization_choices = sorted(
        [(str(org.id), f"{org.name} - {org.id}") for org in organizations],
        key=lambda x: x[1].lower(),
    )
    form.organization_id.choices = organization_choices
    if form.validate_on_submit():
        new_source = make_new_source_contract(form)
        source = deps.db.add_harvest_source(new_source)
        job_message = deps.load_manager.schedule_first_job(source.id)
        if source and job_message:
            _log_mutation(
                "create",
                "harvest_source",
                source.id,
                organization_id=source.organization_id,
                source_name=source.name,
            )
            flash(f"Added new harvest source with ID: {source.id}. {job_message}")
        else:
            flash("Failed to add harvest source.")
        return redirect(url_for("main.harvest_source_list"))
    elif form.errors:
        flash(form.errors)
        return redirect(url_for("main.add_harvest_source"))
    return render_template(
        "edit_data.html",
        form=form,
        action="Add",
        data_type="Harvest Source",
        button="Submit",
    )


@main.route("/harvest_source/<source_id>", methods=["GET"])
@valid_id_required
def view_harvest_source(source_id: str):
    jobs_htmx_vars = {
        "target_div": "#paginated__harvest-jobs",
        "endpoint_url": f"/harvest_source/{source_id}",
        "page_param": "page",
    }
    datasets_htmx_vars = {
        "target_div": "#paginated__datasets",
        "endpoint_url": f"/harvest_source/{source_id}",
        "page_param": "datasets_page",
    }
    harvest_jobs_facets = (
        f"harvest_source_id eq {source_id},date_created le {get_datetime()}"
    )
    jobs_count = deps.db.pget_harvest_jobs(
        facets=harvest_jobs_facets,
        count=True,
    )

    pagination = Pagination(
        count=jobs_count,
        current=request.args.get("page", 1, type=convert_to_int),
    )

    jobs = deps.db.pget_harvest_jobs(
        facets=harvest_jobs_facets,
        page=pagination.db_current,
        order_by="desc",
    )

    if htmx:
        htmx_target = request.headers.get("HX-Target", "")

        if "paginated__datasets" in htmx_target:
            datasets_pagination = Pagination(
                count=deps.db.get_datasets_by_source(source_id=source_id, count=True),
                current=request.args.get("datasets_page", 1, type=convert_to_int),
            )
            datasets = deps.db.get_datasets_by_source(
                source_id=source_id,
                page=datasets_pagination.db_current,
                per_page=datasets_pagination.per_page,
            )
            data = {
                "source": {"id": source_id},
                "datasets": datasets,
                "datasets_htmx_vars": datasets_htmx_vars,
            }
            logger.info(
                "Rendered harvest source datasets partial source_id=%s page=%s "
                "datasets_count=%s returned=%s",
                source_id,
                datasets_pagination.current,
                datasets_pagination.count,
                len(datasets),
            )
            return render_block(
                "view_source_data.html",
                "htmx_paginated_datasets",
                data=data,
                pagination=datasets_pagination.to_dict(),
                datasets_pagination=datasets_pagination.to_dict(),
            )
        else:
            data = {
                "source": {"id": source_id},
                "jobs": jobs,
                "htmx_vars": jobs_htmx_vars,
            }
            logger.info(
                "Rendered harvest source jobs partial source_id=%s page=%s "
                "jobs_count=%s returned=%s",
                source_id,
                pagination.current,
                pagination.count,
                len(jobs),
            )
            return render_block(
                "view_source_data.html",
                "htmx_paginated",
                data=data,
                pagination=pagination.to_dict(),
            )
    else:
        form = HarvestTriggerForm()
        records_count = deps.db.get_latest_harvest_records_by_source_orm(
            source_id=source_id,
            count=True,
        )
        summary_data = {
            "records_count": records_count,
            "last_job_errors": None,
            "last_job_finished": None,
            "next_job_scheduled": None,
            "active_job_in_progress": False,
        }

        if jobs:
            last_job = jobs[0]
            if last_job.status == "in_progress":
                summary_data["active_job_in_progress"] = True

            last_job_error_count = deps.db.get_harvest_record_errors_by_job(
                count=True,
                job_id=last_job.id,
            )
            summary_data["last_job_errors"] = last_job_error_count
            summary_data["last_job_finished"] = last_job.date_finished

        future_jobs = deps.db.get_new_harvest_jobs_by_source_in_future(source_id)

        if future_jobs:
            summary_data["next_job_scheduled"] = future_jobs[0].date_created

        chart_data_values = dynamic_map_list_items_to_dict(
            deps.db._to_dict(jobs[::-1]),
            [
                "date_finished",
                "records_added",
                "records_updated",
                "records_deleted",
                "records_errored",
                "records_ignored",
            ],
        )
        chart_data = {
            "labels": chart_data_values["date_finished"],
            "datasets": [
                {
                    "label": "Added",
                    "data": chart_data_values["records_added"],
                    "borderColor": "green",
                    "backgroundColor": "green",
                },
                {
                    "label": "Updated",
                    "data": chart_data_values["records_updated"],
                    "borderColor": "blue",
                    "backgroundColor": "blue",
                },
                {
                    "label": "Deleted",
                    "data": chart_data_values["records_deleted"],
                    "borderColor": "black",
                    "backgroundColor": "black",
                },
                {
                    "label": "Errored",
                    "data": chart_data_values["records_errored"],
                    "borderColor": "red",
                    "backgroundColor": "red",
                },
                {
                    "label": "Unchanged",
                    "data": chart_data_values["records_ignored"],
                    "borderColor": "grey",
                    "backgroundColor": "grey",
                },
            ],
        }
        source = deps.db.get_harvest_source(source_id)
        datasets_page = request.args.get("datasets_page", 1, type=convert_to_int)
        datasets_count = deps.db.get_datasets_by_source(source_id=source_id, count=True)
        datasets_pagination = Pagination(
            count=datasets_count,
            current=datasets_page,
        )
        datasets = deps.db.get_datasets_by_source(
            source_id=source_id,
            page=datasets_pagination.db_current,
            per_page=datasets_pagination.per_page,
        )
        data = {
            "ckan_url": CKAN_URL,
            "source": source,
            "summary_data": summary_data,
            "jobs": jobs,
            "chart_data": chart_data,
            "htmx_vars": jobs_htmx_vars,
            "datasets": datasets,
            "datasets_htmx_vars": datasets_htmx_vars,
        }
        logger.info(
            "Rendered harvest source page source_id=%s source_name=%s jobs_total=%s "
            "jobs_returned=%s datasets_total=%s datasets_returned=%s "
            "records_count=%s active_job_in_progress=%s",
            source_id,
            getattr(source, "name", None),
            jobs_count,
            len(jobs),
            datasets_count,
            len(datasets),
            records_count,
            summary_data["active_job_in_progress"],
        )
        return render_template(
            "view_source_data.html",
            form=form,
            pagination=pagination.to_dict(),
            datasets_pagination=datasets_pagination.to_dict(),
            data=data,
        )


@main.post("/harvest_source/<source_id>")
@login_required
@valid_id_required
def update_harvest_source_actions(source_id: str):
    """Handle authenticated harvest source action form submissions."""
    form = HarvestTriggerForm()

    if not form.validate_on_submit():
        flash(form.errors)
        return redirect(url_for("main.view_harvest_source", source_id=source_id))

    if form.edit.data:
        return redirect(url_for("main.edit_harvest_source", source_id=source_id))
    elif form.harvest.data:
        if form.force_check.data:
            return trigger_manual_job_helper(source_id, "force_harvest")
        return trigger_manual_job_helper(source_id)
    elif form.clear.data:
        return trigger_manual_job_helper(source_id, "clear")
    elif form.delete.data:
        try:
            message, status = deps.db.delete_harvest_source(source_id)
            _log_mutation("delete", "harvest_source", source_id, status=status)
            flash(message)
            if status == 409:
                return redirect(
                    url_for("main.view_harvest_source", source_id=source_id)
                )
            else:
                return redirect(url_for("main.harvest_source_list"))
        except Exception as e:
            message = f"Failed to delete harvest source :: {repr(e)}"
            logger.error(message)
            flash(message)
            return redirect(url_for("main.view_harvest_source", source_id=source_id))

    return redirect(url_for("main.view_harvest_source", source_id=source_id))


@main.route("/harvest_source_list/", methods=["GET"])
def harvest_source_list():
    sources = deps.db.get_all_harvest_sources()
    data = {"harvest_sources": sources}
    logger.info("Rendered harvest source list count=%s", len(sources) if sources else 0)
    return render_template("view_source_list.html", data=data)


@main.route("/harvest_source/edit/<source_id>", methods=["GET", "POST"])
@login_required
@valid_id_required
def edit_harvest_source(source_id: str):
    if source_id:
        source = deps.db.get_harvest_source(source_id)
        organizations = deps.db.get_all_organizations()
        if source and organizations:
            organization_choices = sorted(
                [
                    (str(org["id"]), f"{org['name']} - {org['id']}")
                    for org in deps.db._to_dict(organizations)
                ],
                key=lambda x: x[1].lower(),
            )
            source_data = deps.db._to_dict(source)
            source_data["notification_emails"] = ", ".join(
                source_data.get("notification_emails") or []
            )
            form = HarvestSourceForm(data=source_data)
            form.organization_id.choices = organization_choices
            if form.validate_on_submit():
                new_source_data = make_new_source_contract(form)
                source = deps.db.update_harvest_source(source_id, new_source_data)
                job_message = deps.load_manager.schedule_first_job(source.id)
                if source and job_message:
                    _log_mutation(
                        "edit",
                        "harvest_source",
                        source.id,
                        organization_id=source.organization_id,
                        source_name=source.name,
                    )
                    flash(f"Updated source with ID: {source.id}. {job_message}")
                else:
                    flash("Failed to update harvest source.")
                return redirect(
                    url_for("main.view_harvest_source", source_id=source.id)
                )
            elif form.errors:
                flash(form.errors)
                return redirect(
                    url_for("main.edit_harvest_source", source_id=source_id)
                )
            return render_template(
                "edit_data.html",
                form=form,
                action="Edit",
                data_type="Harvest Source",
                button="Update",
                source_id=source_id,
            )
        else:
            flash(f"No source with id: {source_id}")
            return redirect(url_for("main.harvest_source_list"))
