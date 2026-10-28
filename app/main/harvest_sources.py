import json

from flask import current_app, flash, redirect, render_template, request, url_for

from app import deps, htmx
from app.deps import (
    CATALOG_BASE_URL,
    CKAN_URL,
    _log_mutation,
    create_harvest_source,
    logger,
    login_required,
    render_block,
    valid_id_required,
)
from app.forms import HarvestSourceForm, HarvestTriggerForm, ReharvestForm
from app.paginate import Pagination
from app.util import make_new_source_contract
from database.interface import HarvestSourceLockedError, StaleReharvestLinkError
from harvester.lib.source_delete import enqueue_harvest_source_delete
from harvester.utils.general_utils import (
    convert_to_int,
    dynamic_map_list_items_to_dict,
    get_datetime,
)
from harvester.utils.notification_frequency import notification_frequency_notice
from harvester.utils.reharvest_links import read_reharvest_token

from . import main


def trigger_manual_job_helper(source_id, job_type="harvest"):
    message, category = deps.load_manager.trigger_manual_job(source_id, job_type)
    flash(message, category)
    return redirect(url_for("main.view_harvest_source", source_id=source_id))


@main.route("/harvest_source/refresh", methods=["GET", "POST"])
def refresh_harvest_source():
    """Confirm a signed notification link before requesting a normal harvest."""
    token = request.values.get("token", "")
    payload = read_reharvest_token(current_app.config["SECRET_KEY"], token)
    if payload is None:
        return (
            render_template(
                "refresh_harvest_source.html",
                message="Invalid link.",
                category="error",
                explanation=(
                    "This link could not be verified. Open the complete link from "
                    "your notification email, or contact the Data.gov team for help."
                ),
            ),
            400,
        )

    source_id = payload["source_id"]
    report_job_id = payload["job_id"]
    recipient = payload["email"]

    def audit(outcome, new_job_id=None):
        logger.info(
            "Audit reharvest recipient=%s source_id=%s report_job_id=%s "
            "new_job_id=%s outcome=%s",
            recipient,
            source_id,
            report_job_id,
            new_job_id,
            outcome,
        )

    def locked_issue():
        return (
            "Harvesting is locked for this source.",
            "Contact the Data.gov team to have it unlocked. You can retry this "
            "link after it is unlocked.",
            409,
            "locked",
        )

    def link_issue():
        """Describe the current reason this signed link cannot request a job."""
        source = deps.db.get_harvest_source(source_id)
        if source is None:
            return None, (
                "This harvest source has been removed.",
                "Contact the Data.gov team if this source should be restored "
                "or harvested again.",
                410,
                "stale",
            )

        latest_job = deps.db.get_latest_harvest_job_for_source(source_id)
        if latest_job is None:
            return source, (
                "We could not find the harvest job associated with this link.",
                "Contact the Data.gov team for help requesting another harvest.",
                410,
                "stale",
            )
        if latest_job.id != report_job_id:
            linked_job = deps.db.get_harvest_job(report_job_id)
            if linked_job is None or linked_job.harvest_source_id != source_id:
                return source, (
                    "We could not find the harvest job associated with this link.",
                    "Contact the Data.gov team for help requesting another harvest.",
                    410,
                    "stale",
                )
            if latest_job.status in ("complete", "error") and latest_job.date_finished:
                explanation = (
                    "A newer harvest job has finished. If you received a newer "
                    "notification email, use the link in that email; otherwise, "
                    "contact the Data.gov team."
                )
            else:
                explanation = (
                    "A newer harvest job is queued or running. Wait for it to "
                    "finish. If you receive a newer notification email, use the "
                    "link in that email; otherwise, contact the Data.gov team."
                )
            return source, (
                "This link is from an earlier harvest notification.",
                explanation,
                410,
                "stale",
            )
        if (
            latest_job.status not in ("complete", "error")
            or latest_job.date_finished is None
        ):
            return source, (
                "The harvest job associated with this link is not recorded "
                "as finished.",
                "Wait for it to finish. If it is already complete, contact "
                "the Data.gov team.",
                410,
                "stale",
            )
        if source.is_locked:
            return source, locked_issue()
        return source, None

    def unavailable_response(source, issue):
        message, explanation, status, outcome = issue
        if request.method == "POST":
            audit(outcome)
        frequency_notice = (
            notification_frequency_notice(source.notification_frequency)
            if source is not None
            and message == "This link is from an earlier harvest notification."
            else None
        )
        return (
            render_template(
                "refresh_harvest_source.html",
                source=source,
                message=message,
                category="error",
                explanation=explanation,
                frequency_notice=frequency_notice,
            ),
            status,
        )

    source, issue = link_issue()
    if issue is not None:
        return unavailable_response(source, issue)

    form = ReharvestForm()
    if request.method == "POST":
        if not form.validate_on_submit():
            audit("invalid_form")
            return (
                render_template(
                    "refresh_harvest_source.html",
                    message="Invalid request.",
                    category="error",
                ),
                400,
            )
        try:
            job, active_job = deps.db.enqueue_harvest_job_if_idle(
                source_id,
                job_type="harvest",
                expected_latest_job_id=report_job_id,
            )
        except HarvestSourceLockedError:
            return unavailable_response(source, locked_issue())
        except StaleReharvestLinkError:
            source, issue = link_issue()
            return unavailable_response(
                source,
                issue
                or (
                    "The harvest source changed while your request was "
                    "being processed.",
                    "Open the link again to check its current status.",
                    410,
                    "stale",
                ),
            )
        except ValueError:
            # The source may have been deleted after the initial page check.
            source, issue = link_issue()
            if source is None:
                return unavailable_response(source, issue)
            raise
        if active_job:
            audit("already_active", active_job.id)
            return (
                render_template(
                    "refresh_harvest_source.html",
                    source=source,
                    message=f"Job {active_job.id} is already queued or running.",
                    category="warning",
                ),
                409,
            )

        result, category = deps.load_manager.start_job(job.id, "harvest")
        audit("requested", job.id)
        return render_template(
            "refresh_harvest_source.html",
            source=source,
            message=result,
            category=category,
            new_job_id=job.id,
        )

    return render_template(
        "refresh_harvest_source.html", source=source, form=form, token=token
    )


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
    if request.method == "GET":
        # "Save and add another" returns here with the previous organization
        requested_org = request.args.get("organization_id")
        if requested_org in dict(organization_choices):
            form.organization_id.data = requested_org
    if form.validate_on_submit():
        try:
            source_data = make_new_source_contract(form)
            source, message, status = create_harvest_source(source_data)
        except Exception as e:
            message = "Failed to add harvest source."
            logger.error(f"{message} :: {repr(e)}")
            flash(message, "error")
            return redirect(url_for("main.add_harvest_source"))
        if source is None:
            category = "error"
        elif status != 200:
            # created, but scheduling its first harvest failed
            category = "warning"
        else:
            category = "success"
        flash(message, category)
        if source is None:
            return redirect(url_for("main.add_harvest_source"))
        if request.form.get("add_another"):
            return redirect(
                url_for(
                    "main.add_harvest_source",
                    organization_id=source.organization_id,
                )
            )
        return redirect(url_for("main.view_harvest_source", source_id=source.id))
    elif form.errors:
        flash(form.errors, "error")
        return redirect(url_for("main.add_harvest_source"))
    return render_template(
        "edit_data.html",
        form=form,
        action="Add",
        data_type="Harvest Source",
        button="Submit",
        add_another=True,
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
                "catalog_base_url": CATALOG_BASE_URL,
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
        # open issues right now, not just what the last job wrote (#6440)
        issue_record_count = deps.db.get_current_record_issues_by_source(
            source_id, for_view=True, count=True
        )
        issue_summary = deps.db.get_record_issues_summary_by_source(source_id)
        issue_error_total = sum(
            row["count"] for row in issue_summary if row["severity"] == "error"
        )
        issue_warning_total = sum(
            row["count"] for row in issue_summary if row["severity"] == "warning"
        )
        summary_data = {
            "records_count": records_count,
            "issue_record_count": issue_record_count,
            "issue_error_total": issue_error_total,
            "issue_warning_total": issue_warning_total,
            "last_job_finished": None,
            "next_job_scheduled": None,
            "active_job_in_progress": False,
        }

        if jobs:
            last_job = jobs[0]
            if last_job.status == "in_progress":
                summary_data["active_job_in_progress"] = True

            summary_data["last_job_finished"] = last_job.date_finished

        source = deps.db.get_harvest_source(source_id)
        summary_data["next_job_scheduled"] = source.date_next_run if source else None

        chart_data_values = dynamic_map_list_items_to_dict(
            deps.db._to_dict(jobs[::-1]),
            [
                "date_finished",
                "records_added",
                "records_updated",
                "records_deleted",
                "records_errored",
                "records_warned",
                "records_ignored",
            ],
        )
        chart_data = {
            "labels": chart_data_values["date_finished"],
            "datasets": [
                {
                    "label": "Added",
                    "data": chart_data_values["records_added"],
                    "borderColor": "#2e7044",
                    "backgroundColor": "#4f9f62",
                },
                {
                    "label": "Updated",
                    "data": chart_data_values["records_updated"],
                    "borderColor": "#005ea8",
                    "backgroundColor": "#2f8ec5",
                },
                {
                    "label": "Deleted",
                    "data": chart_data_values["records_deleted"],
                    "borderColor": "#8b4b13",
                    "backgroundColor": "#b87535",
                },
                {
                    "label": "Errored",
                    "data": chart_data_values["records_errored"],
                    "borderColor": "#b50909",
                    "backgroundColor": "#d54343",
                },
                {
                    "label": "Warned",
                    "data": chart_data_values["records_warned"],
                    "borderColor": "#7d5f00",
                    "backgroundColor": "#b38b00",
                },
                {
                    "label": "Unchanged",
                    "data": chart_data_values["records_ignored"],
                    "borderColor": "#565c65",
                    "backgroundColor": "#7f858c",
                },
            ],
        }
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
            "catalog_base_url": CATALOG_BASE_URL,
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


def _load_json_title(json_string, issue_types):
    try:
        if json_string is None:
            return None
        if "TransformationException" in issue_types:
            # can't json load an xml doc
            return None
        return json.loads(json_string).get("title", None)
    except Exception as e:
        logger.error(f"Error loading json source_raw: {repr(e)}")
        return None


@main.route("/harvest_source/<source_id>/warnings", methods=["GET"])
@valid_id_required
def view_harvest_source_warnings(source_id: str):
    source = deps.db.get_harvest_source(source_id)
    show_record_type = bool(source and source.schema_type == "dcatus3.0")

    record_error_count = deps.db.get_current_record_issues_by_source(
        source_id, for_view=True, count=True
    )
    htmx_vars = {
        "target_div": "#error_results_pagination",
        "endpoint_url": f"/harvest_source/{source_id}/warnings",
        "page_param": "page",
    }
    pagination = Pagination(
        count=record_error_count,
        current=request.args.get("page", 1, type=convert_to_int),
    )
    record_error_summary = deps.db.get_record_issues_summary_by_source(source_id)
    error_total = sum(
        row["count"] for row in record_error_summary if row["severity"] == "error"
    )
    warning_total = sum(
        row["count"] for row in record_error_summary if row["severity"] == "warning"
    )
    record_errors = deps.db.get_current_record_issues_by_source(
        source_id, page=pagination.db_current, for_view=True
    )
    record_errors_dict = [
        {
            "identifier": record["identifier"] or record["harvest_record_id"],
            "harvest_record_id": record["harvest_record_id"],
            "record_type": record["record_type"] if show_record_type else None,
            "title": _load_json_title(
                record["title_source"], [issue["type"] for issue in record["issues"]]
            ),
            "date_created": record["date_created"],
            "issues": record["issues"],
        }
        for record in record_errors
    ]

    if htmx:
        data = {
            "source": {"id": source_id},
            "record_errors": record_errors_dict,
            "htmx_vars": htmx_vars,
        }
        logger.info(
            "Rendered harvest source warnings partial source_id=%s page=%s "
            "error_count=%s returned=%s",
            source_id,
            pagination.current,
            pagination.count,
            len(record_errors_dict),
        )
        return render_block(
            "view_source_warnings.html",
            "record_errors_table",
            data=data,
            pagination=pagination.to_dict(),
        )
    else:
        data = {
            "source": source,
            "record_error_summary": record_error_summary,
            "record_count": record_error_count,
            "error_total": error_total,
            "warning_total": warning_total,
            "record_errors": record_errors_dict,
            "htmx_vars": htmx_vars,
        }
        logger.info(
            "Rendered harvest source warnings page source_id=%s error_count=%s "
            "returned=%s page=%s",
            source_id,
            record_error_count,
            len(record_errors_dict),
            pagination.current,
        )
        return render_template(
            "view_source_warnings.html", data=data, pagination=pagination.to_dict()
        )


@main.post("/harvest_source/<source_id>")
@login_required
@valid_id_required
def update_harvest_source_actions(source_id: str):
    """Handle authenticated harvest source action form submissions."""
    form = HarvestTriggerForm()

    if not form.validate_on_submit():
        flash(form.errors, "error")
        return redirect(url_for("main.view_harvest_source", source_id=source_id))

    if form.edit.data:
        return redirect(url_for("main.edit_harvest_source", source_id=source_id))
    elif form.lock.data or form.unlock.data:
        locked = bool(form.lock.data)
        source = deps.db.set_harvest_source_lock(source_id, locked)
        if source is None:
            flash("Harvest source not found.", "error")
        else:
            action = "lock" if locked else "unlock"
            _log_mutation(action, "harvest_source", source_id)
            flash(
                f"Harvest source {source_id} {'locked' if locked else 'unlocked'}.",
                "success",
            )
        return redirect(url_for("main.view_harvest_source", source_id=source_id))
    elif form.harvest.data:
        if form.force_check.data:
            return trigger_manual_job_helper(source_id, "force_harvest")
        return trigger_manual_job_helper(source_id)
    elif form.clear.data:
        return trigger_manual_job_helper(source_id, "clear")
    elif form.delete.data:
        try:
            message, status = enqueue_harvest_source_delete(source_id, deps.db)
            _log_mutation("delete", "harvest_source", source_id, status=status)
            # 202: the delete task was scheduled; anything else was refused
            flash(message, "success" if status == 202 else "error")
            if status == 409:
                return redirect(
                    url_for("main.view_harvest_source", source_id=source_id)
                )
            return redirect(url_for("main.harvest_source_list"))
        except Exception as e:
            message = f"Failed to delete harvest source :: {repr(e)}"
            logger.error(message)
            flash(message, "error")
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
            source_data["send_report_email"] = str(
                source_data.get("send_report_email", False)
            )
            form = HarvestSourceForm(data=source_data)
            form.organization_id.choices = organization_choices
            if form.validate_on_submit():
                old_frequency = source.frequency
                new_source_data = make_new_source_contract(form)

                source, error = deps.db.try_update_harvest_source(
                    source_id, new_source_data
                )
                if not source:
                    flash(error or "Failed to update harvest source.", "error")
                    return redirect(
                        url_for("main.edit_harvest_source", source_id=source_id)
                    )

                job_message = ""
                if source.frequency != old_frequency:
                    job_message = deps.load_manager.reschedule_next_run(source.id)
                _log_mutation(
                    "edit",
                    "harvest_source",
                    source.id,
                    organization_id=source.organization_id,
                    source_name=source.name,
                )
                flash(f"Updated source with ID: {source.id}. {job_message}", "success")
                return redirect(
                    url_for("main.view_harvest_source", source_id=source.id)
                )
            elif form.errors:
                flash(form.errors, "error")
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
            flash(f"No source with id: {source_id}", "error")
            return redirect(url_for("main.harvest_source_list"))
