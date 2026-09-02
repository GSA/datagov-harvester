import logging
from typing import Optional

logger = logging.getLogger(__name__)


def format_email(email: Optional[str]) -> Optional[str]:
    if not email:
        return None
    if email.startswith("mailto:"):
        return email
    return f"mailto:{email}"


def get_bureau_code_for_agency(agency: str) -> Optional[str]:
    AGENCY_BUREAU_CODES = {
        "USDA": "005",
        "DOC": "006",
        "DOD": "017",
        "ED": "091",
        "DOE": "089",
        "HHS": "075",
        "DHS": "070",
        "HUD": "086",
        "DOI": "010",
        "DOJ": "015",
        "DOL": "016",
        "DOS": "019",
        "DOT": "069",
        "TREAS": "020",
        "VA": "036",
        "EPA": "020",
        "GSA": "023",
        "NASA": "026",
        "NARA": "027",
        "NSF": "049",
        "OPM": "024",
        "SBA": "073",
        "SSA": "028",
        "USAID": "072",
        "NRC": "031",
        "SEC": "050",
        "USPS": "018",
        "FCC": "027",
        "FDIC": "086",
        "FTC": "029",
    }
    return AGENCY_BUREAU_CODES.get(agency.upper())


def codejson_release_to_dcat(release: dict, agency: str, organization_id: str) -> dict:
    dcat_dataset = {
        "@type": "dcat:Dataset",
        "identifier": release["repositoryURL"],
        "title": release["name"],
        "description": release.get("description", "No description provided"),
        "accessLevel": "public",
        "keyword": release.get("tags", []),
        "theme": release.get("languages", []),
    }

    landing_url = release.get("homepageURL", release["repositoryURL"])
    dcat_dataset["landingPage"] = {
        "@id": landing_url,
        "@type": "Document",
        "title": f"{release['name']} Homepage",
        "accessURL": landing_url,
    }

    publisher_name = release.get("organization", agency)
    dcat_dataset["publisher"] = {
        "@type": "org:Organization",
        "name": publisher_name,
    }
    if release.get("organization"):
        dcat_dataset["publisher"]["subOrganizationOf"] = [
            {
                "@type": "org:Organization",
                "name": agency,
            }
        ]

    contact = release.get("contact", {})
    dcat_dataset["contactPoint"] = {
        "@type": "vcard:Contact",
        "fn": contact.get("URL", "Unknown"),
        "hasEmail": format_email(contact.get("email")),
    }

    dates = release.get("date", {})
    if dates.get("created"):
        dcat_dataset["issued"] = dates["created"]
    if dates.get("lastModified"):
        dcat_dataset["modified"] = dates["lastModified"]

    permissions = release.get("permissions", {})
    licenses = permissions.get("licenses", [])
    if licenses:
        dcat_dataset["license"] = licenses[0].get("URL") or licenses[0].get("name")

    vcs = release.get("vcs", "git")
    distribution = {
        "@type": "dcat:Distribution",
        "accessURL": release["repositoryURL"],
        "title": f"{release['name']} Repository",
        "format": vcs,
    }
    if release.get("downloadURL"):
        distribution["downloadURL"] = release["downloadURL"]
    dcat_dataset["distribution"] = [distribution]

    dcat_dataset["codejson"] = {
        "status": release.get("status"),
        "laborHours": release.get("laborHours"),
        "vcs": vcs,
        "usageType": permissions.get("usageType"),
    }

    bureau_code = get_bureau_code_for_agency(agency)
    if bureau_code:
        dcat_dataset["bureauCode"] = [bureau_code]

    return dcat_dataset
