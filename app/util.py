# Helper Functions
def make_new_source_contract(form):
    return {
        "organization_id": form.organization_id.data,
        "name": form.name.data,
        "url": form.url.data,
        "notification_emails": form.notification_emails.data,
        "frequency": form.frequency.data,
        "schema_type": form.schema_type.data,
        "source_type": form.source_type.data,
        "notification_frequency": form.notification_frequency.data,
    }


def make_new_record_error_contract(error: tuple) -> dict:
    """
    convert the record error row tuple into a dict. splits the validation message
    value into an array
    """
    fields = [
        "harvest_record_id",
        "harvest_job_id",
        "date_created",
        "type",
        "message",
        "id",
    ]

    # identifier and source_raw are the last 2 and kept the same
    record_error = dict(zip(fields, error[:-2]))
    error_type = error[3]
    if error_type in ["ValidationException", "ValidationError"]:
        record_error["message"] = record_error["message"].split("::")  # turn into array

    return record_error


def make_new_org_contract(form):
    return {
        "name": form.name.data,
        "slug": form.slug.data,
        "logo": form.logo.data,
        "description": form.description.data or None,
        "organization_type": form.organization_type.data or None,
        "aliases": [alias.strip() for alias in (form.aliases.data or "").split(",")],
    }
