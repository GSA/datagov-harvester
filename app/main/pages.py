import json
from datetime import timedelta

from flask import flash, redirect, render_template, request, session, url_for

from app import deps, htmx
from app.deps import (
    _log_mutation,
    logger,
    render_block,
)
from app.forms import DatasetSlugForm, ValidatorForm
from app.paginate import Pagination
from app.util import fetch_json_from_url, validate_records
from harvester.utils.general_utils import (
    convert_to_int,
    get_datetime,
    process_job_complete_percentage,
)

from . import main


@main.route("/", methods=["GET"])
def index():
    return redirect(url_for("main.organization_list"))


@main.route("/dataset/<string:dataset_slug>", methods=["GET", "POST"])
def view_dataset(dataset_slug: str):
    """View a dataset detail page by slug, and handle slug edits via POST."""
    dataset = deps.db.get_dataset_by_slug(dataset_slug)

    if request.method == "POST":
        if not session.get("user"):
            flash("You must be logged in to edit a dataset slug.")
            return redirect(url_for("main.view_dataset", dataset_slug=dataset_slug))

        form = DatasetSlugForm(
            request.form,
            dataset_id=dataset.id if dataset else None,
            db_interface=deps.db,
        )
        if dataset is None:
            flash("Dataset not found.")
            return redirect(url_for("main.harvest_source_list"))

        if form.validate():
            updated, os_synced, os_error = deps.db.update_dataset_slug(
                dataset.id, form.slug.data
            )
            if updated:
                _log_mutation(
                    "edit",
                    "dataset",
                    updated.id,
                    old_slug=dataset_slug,
                    new_slug=updated.slug,
                    opensearch_synced=os_synced,
                )
                if os_synced:
                    flash(
                        f"Slug updated successfully to '{updated.slug}'.",
                        "success",
                    )
                else:
                    flash(
                        f"Slug updated to '{updated.slug}' in the database, "
                        "but the search index could not be updated. "
                        f"Error: {os_error}",
                        "warning",
                    )
                return redirect(url_for("main.view_dataset", dataset_slug=updated.slug))
            else:
                flash(
                    f"Failed to update slug. Error: {os_error}",
                    "danger",
                )
                return redirect(url_for("main.view_dataset", dataset_slug=dataset_slug))
        else:
            data = {"dataset": dataset}
            return (
                render_template(
                    "view_dataset_data.html",
                    data=data,
                    form=form,
                ),
                422,
            )

    form = DatasetSlugForm(
        dataset_id=dataset.id if dataset else None,
        db_interface=deps.db,
        data={"slug": dataset.slug} if dataset else {},
    )
    data = {"dataset": dataset}
    return render_template(
        "view_dataset_data.html",
        data=data,
        form=form,
    ), (200 if dataset is not None else 404)


@main.route("/metrics/", methods=["GET"])
def view_metrics():
    """Render index page with recent harvest jobs."""
    current_time = get_datetime()
    start_time = current_time - timedelta(days=7)
    time_filter = (
        f"date_created ge {start_time.isoformat()},date_created le {current_time}"
    )

    jobs_page = request.args.get("jobs_page", 1, type=convert_to_int)
    errors_page = request.args.get("errors_page", 1, type=convert_to_int)

    count_jobs = deps.db.pget_harvest_jobs(
        facets=time_filter + ",status eq complete",
        count=True,
    )

    count_errors = deps.db.pget_harvest_job_errors(
        facets=time_filter,
        count=True,
    )

    pagination_jobs = Pagination(
        count=count_jobs,
        current=jobs_page,
    )

    pagination_errors = Pagination(
        count=count_errors,
        current=errors_page,
    )

    if htmx:
        htmx_target = request.headers.get("HX-Target", "")

        if "paginated__harvest-jobs" in htmx_target:
            jobs = deps.db.pget_harvest_jobs(
                facets=time_filter + ",status eq complete",
                page=pagination_jobs.db_current,
                per_page=pagination_jobs.per_page,
                order_by="desc",
            )
            htmx_vars = {
                "target_div": "#paginated__harvest-jobs",
                "endpoint_url": "/metrics/",
                "page_param": "jobs_page",
            }
            data = {
                "jobs": jobs,
                "htmx_vars": htmx_vars,
            }
            logger.info(
                "Rendered metrics jobs partial page=%s total_jobs=%s returned=%s "
                "window_start=%s window_end=%s",
                pagination_jobs.current,
                count_jobs,
                len(jobs),
                start_time.isoformat(),
                current_time.isoformat(),
            )
            return render_block(
                "metrics_dashboard.html",
                "htmx_paginated_jobs",
                data=data,
                pagination_jobs=pagination_jobs.to_dict(),
            )

        elif "paginated__harvest-errors" in htmx_target:
            errors_time_filter = (
                f"date_created ge {start_time.isoformat()},"
                f"date_created le {current_time}"
            )
            failures = deps.db.pget_harvest_job_errors(
                facets=errors_time_filter,
                page=pagination_errors.db_current,
                per_page=pagination_errors.per_page,
                order_by="desc",
            )
            htmx_vars = {
                "target_div": "#paginated__harvest-errors",
                "endpoint_url": "/metrics/",
                "page_param": "errors_page",
            }
            data = {
                "failures": failures,
                "htmx_vars": htmx_vars,
            }
            logger.info(
                "Rendered metrics errors partial page=%s total_errors=%s returned=%s "
                "window_start=%s window_end=%s",
                pagination_errors.current,
                count_errors,
                len(failures),
                start_time.isoformat(),
                current_time.isoformat(),
            )
            return render_block(
                "metrics_dashboard.html",
                "htmx_paginated_errors",
                data=data,
                pagination_errors=pagination_errors.to_dict(),
            )
    else:
        jobs = deps.db.pget_harvest_jobs(
            facets=time_filter + ",status eq complete",
            page=pagination_jobs.db_current,
            per_page=pagination_jobs.per_page,
            order_by="desc",
        )
        errors_time_filter = (
            f"date_created ge {start_time.isoformat()},date_created le {current_time}"
        )
        failures = deps.db.pget_harvest_job_errors(
            facets=errors_time_filter,
            page=pagination_errors.db_current,
            per_page=pagination_errors.per_page,
            order_by="desc",
        )
        current_jobs = deps.db.pget_harvest_jobs(
            facets="status eq in_progress",
            order_by="desc",
        )

        for job in current_jobs:
            job.percent_complete = process_job_complete_percentage(job.to_dict())

        data = {
            "current_jobs": current_jobs,
            "jobs": jobs,
            "new_jobs_in_past": deps.db.get_new_harvest_jobs_in_past(),
            "failures": failures,
            "current_time": current_time,
            "window_start": start_time,
        }
        logger.info(
            "Rendered metrics page current_jobs=%s completed_jobs_total=%s "
            "completed_jobs_returned=%s failure_total=%s failure_returned=%s "
            "window_start=%s window_end=%s",
            len(current_jobs),
            count_jobs,
            len(jobs),
            count_errors,
            len(failures),
            start_time.isoformat(),
            current_time.isoformat(),
        )
        return render_template(
            "metrics_dashboard.html",
            pagination_jobs=pagination_jobs.to_dict(),
            pagination_errors=pagination_errors.to_dict(),
            data=data,
        )


@main.route("/openapi/docs")
def openapi_docs():
    return render_template("/swagger.html")


@main.route("/validate/", methods=["GET", "POST"])
def view_validators():
    """View for validating v1.1 or v3.0 dcatus catalogs using form."""
    errors = []
    submitted = False

    form = ValidatorForm()
    if form.validate_on_submit():
        data = []
        if form.fetch_method.data == "url":
            try:
                data = fetch_json_from_url(form.url.data)
            except Exception as e:
                form.url.errors.append(str(e))
        elif form.fetch_method.data == "paste":
            try:
                data = json.loads(form.json_text.data)
            except Exception as e:
                form.json_text.errors.append(str(e))
        elif form.fetch_method.data == "upload":
            try:
                raw = form.json_file.data.read()
                data = json.loads(raw)
            except json.JSONDecodeError as e:
                form.json_file.errors.append(f"Invalid JSON in uploaded file: {e}")
            except Exception as e:
                form.json_file.errors.append(str(e))

        if not form.errors:
            submitted = True
            errors = validate_records(data, form.schema.data)
            logger.info(
                "Rendered validator results fetch_method=%s schema=%s "
                "validation_errors=%s",
                form.fetch_method.data,
                form.schema.data,
                len(errors),
            )
        else:
            logger.warning(
                "Validator submission failed fetch_method=%s url_errors=%s "
                "json_errors=%s",
                form.fetch_method.data,
                len(form.url.errors),
                len(form.json_text.errors),
            )

    template_data = {
        "record_errors": errors,
    }

    if request.method == "GET":
        logger.info("Rendered validator page")

    return render_template(
        "view_validators.html",
        form=form,
        data=template_data,
        submitted=submitted,
    )
