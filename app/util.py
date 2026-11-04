# Helper Functions
def make_new_source_contract(form):
    collection_parent_url = None
    if form.source_type.data == "waf-collection":
        collection_parent_url = form.collection_parent_url.data

    return {
        "organization_id": form.organization_id.data,
        "name": form.name.data,
        "url": form.url.data,
        "notification_emails": form.notification_emails.data,
        "frequency": form.frequency.data,
        "schema_type": form.schema_type.data,
        "source_type": form.source_type.data,
        "collection_parent_url": collection_parent_url,
        "notification_frequency": form.notification_frequency.data,
        "send_report_email": form.send_report_email.data == "True",
    }


def make_new_org_contract(form):
    # Convert empty string to None for code_repo_url
    code_repo_url = form.code_repo_url.data
    if code_repo_url:
        code_repo_url = code_repo_url.strip() or None
    else:
        code_repo_url = None

    return {
        "name": form.name.data,
        "slug": form.slug.data,
        "logo": form.logo.data or None,
        "description": form.description.data or None,
        "organization_type": form.organization_type.data or None,
        "aliases": [alias.strip() for alias in (form.aliases.data or "").split(",")],
        "code_repo_url": code_repo_url,
        "code_repo_exempt": form.code_repo_exempt.data or False,
    }
