import json

from flask import render_template, request

from app import deps, htmx
from app.deps import CKAN_URL, logger, render_block, valid_id_required
from app.paginate import Pagination
from app.util import make_new_record_error_contract
from harvester.utils.general_utils import (
    convert_to_int,
    group_record_error_fields,
    process_job_complete_percentage,
)

from . import main


def _load_json_title(json_string, error_type):
    try:
        if json_string is None:
            logger.warning("harvest record doesn't have a source to load")
            return
        if error_type == "TransformationException":
            logger.warning(
                "transformation error produced no source_transform. "
                "can't json load an xml doc"
            )
            return
        return json.loads(json_string).get("title", None)
    except Exception as e:
        logger.error(f"Error loading json source_raw: {repr(e)}")
        return


@main.route("/harvest_job/<job_id>", methods=["GET"])
@valid_id_required
def view_harvest_job(job_id=None):
    record_error_count = deps.db.get_harvest_record_errors_by_job(
        job_id,
        count=True,
    )
    htmx_vars = {
        "target_div": "#error_results_pagination",
        "endpoint_url": f"/harvest_job/{job_id}",
        "page_param": "page",
    }

    pagination = Pagination(
        count=record_error_count,
        current=request.args.get("page", 1, type=convert_to_int),
    )
    record_error_summary = deps.db.get_record_errors_summary_by_job(job_id)
    record_errors = deps.db.get_harvest_record_errors_by_job(
        job_id, page=pagination.db_current, for_view=True
    )
    record_errors_dict = [
        {
            "error": make_new_record_error_contract(row),
            "identifier": row[-2],
            "title": _load_json_title(row[-1], row[3]),
        }
        for row in record_errors
    ]

    if htmx:
        data = {
            "harvest_job_id": job_id,
            "record_errors": record_errors_dict,
            "htmx_vars": htmx_vars,
        }
        logger.info(
            "Rendered harvest job errors partial job_id=%s page=%s error_count=%s "
            "returned=%s",
            job_id,
            pagination.current,
            pagination.count,
            len(record_errors_dict),
        )
        return render_block(
            "view_job_data.html",
            "record_errors_table",
            data=data,
            pagination=pagination.to_dict(),
        )
    else:
        job = deps.db.get_harvest_job(job_id)
        data = {
            "job": job,
            "record_error_summary": record_error_summary,
            "record_errors": record_errors_dict,
            "htmx_vars": htmx_vars,
        }
        if job and job.status == "in_progress":
            data["percent_complete"] = process_job_complete_percentage(job.to_dict())
        logger.info(
            "Rendered harvest job detail job_id=%s status=%s source_id=%s "
            "record_error_count=%s record_errors_returned=%s page=%s",
            job_id,
            getattr(job, "status", None),
            getattr(job, "harvest_source_id", None),
            record_error_count,
            len(record_errors_dict),
            pagination.current,
        )

        return render_template(
            "view_job_data.html", data=data, pagination=pagination.to_dict()
        )


@main.route("/harvest_job/<job_id>/report", methods=["GET"])
@valid_id_required
def view_harvest_job_report(job_id=None):
    job = deps.db.get_harvest_job(job_id)
    if not job:
        return render_template("view_harvest_job_report.html", data={"job": None})

    error_field_summary = group_record_error_fields(
        deps.db.get_record_error_messages_summary_by_job(job_id)
    )
    sample_datasets = deps.db.get_datasets_by_source(
        job.harvest_source_id, page=0, per_page=10
    )

    data = {
        "job": job,
        "ckan_url": CKAN_URL,
        "error_field_summary": error_field_summary,
        "sample_datasets": sample_datasets,
    }
    logger.info(
        "Rendered harvest job report job_id=%s status=%s source_id=%s "
        "field_issue_types=%s sample_datasets=%s",
        job_id,
        job.status,
        job.harvest_source_id,
        len(error_field_summary),
        len(sample_datasets),
    )

    return render_template("view_harvest_job_report.html", data=data)
