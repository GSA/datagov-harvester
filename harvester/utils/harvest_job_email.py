"""Render the complete HTML version of a harvest notification email."""

from pathlib import Path
from urllib.parse import quote

from jinja2 import Environment, FileSystemLoader, select_autoescape

_TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "templates"
_SUBSCRIPTION_FOOTER = (
    "You received this email because you subscribed to harvester updates."
)
_ENV = Environment(
    loader=FileSystemLoader(_TEMPLATE_DIR),
    autoescape=select_autoescape(["html"]),
)

_SUMMARY_FIELDS = (
    ("Records Added", "records_added"),
    ("Records Updated", "records_updated"),
    ("Records Deleted", "records_deleted"),
    ("Records Unchanged", "records_ignored"),
    ("Records Errored", "records_errored"),
    ("Records Warned", "records_warned"),
    ("Records Validated", "records_validated"),
)


def render_harvest_job_email_html(
    *,
    title,
    introduction,
    source_name,
    job_id,
    job_url,
    refresh_url,
    organization_name=None,
    job_results=None,
    error_message=None,
    error_field_summary=None,
    sample_datasets=None,
    catalog_base_url=None,
    footer_message=_SUBSCRIPTION_FOOTER,
):
    """Render all email content; Jinja escapes names, errors, and URLs."""
    summary = (
        [(label, job_results[key]) for label, key in _SUMMARY_FIELDS]
        if job_results is not None
        else None
    )
    datasets = None
    if sample_datasets is not None:
        datasets = []
        for dataset in sample_datasets:
            slug = str(dataset.slug)
            url = (
                f"{catalog_base_url.rstrip('/')}/dataset/{quote(slug, safe='')}"
                if catalog_base_url
                else None
            )
            datasets.append({"slug": slug, "url": url})

    return _ENV.get_template("harvest_job_email.html").render(
        title=title,
        introduction=introduction,
        organization_name=organization_name,
        source_name=source_name,
        job_id=job_id,
        job_url=job_url,
        refresh_url=refresh_url,
        summary=summary,
        error_message=error_message,
        error_field_summary=error_field_summary,
        sample_datasets=datasets,
        footer_message=footer_message,
    )
