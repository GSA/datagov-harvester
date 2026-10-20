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

_NOTIFICATION_FREQUENCY_LABELS = {
    "always": "Always",
    "on_error": "On error",
    "on_error_or_update": "On error or update",
}


def reharvest_link_guidance(notification_frequency):
    """Keep the plain-text and HTML link guidance in sync."""
    guidance = (
        "Use the link in your most recent notification for this source. A newer "
        "harvest job expires links in earlier notification emails."
    )
    label = _NOTIFICATION_FREQUENCY_LABELS.get(notification_frequency)
    if label:
        guidance += f" Notification frequency: {label}."
    if notification_frequency != "always":
        guidance += (
            " Ask the Data.gov team to set it to Always if you need a link "
            "after every finished job."
        )
    return guidance


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
    notification_frequency=None,
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
        link_guidance=reharvest_link_guidance(notification_frequency),
    )
