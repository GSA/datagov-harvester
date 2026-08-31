"""
Transformation utilities for converting code.json releases to DCAT-US 3.0 datasets.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def format_email(email: Optional[str]) -> Optional[str]:
    """
    Ensure email has mailto: prefix.

    Args:
        email: Email address string or None

    Returns:
        Email with mailto: prefix, or None if input is None/empty
    """
    if not email:
        return None
    if email.startswith("mailto:"):
        return email
    return f"mailto:{email}"


def get_bureau_code_for_agency(agency: str) -> Optional[str]:
    """
    Map agency acronym to OMB bureau code.

    Bureau codes are 3-digit codes assigned by OMB for federal agencies.
    Source: https://www.usaspending.gov/data-dictionary

    Args:
        agency: Agency acronym (e.g., "DHS", "NASA")

    Returns:
        Bureau code string or None if agency not found
    """
    AGENCY_BUREAU_CODES = {
        # Cabinet Departments
        "USDA": "005",  # Department of Agriculture
        "DOC": "006",  # Department of Commerce
        "DOD": "017",  # Department of Defense
        "ED": "091",  # Department of Education
        "DOE": "089",  # Department of Energy
        "HHS": "075",  # Department of Health and Human Services
        "DHS": "070",  # Department of Homeland Security
        "HUD": "086",  # Department of Housing and Urban Development
        "DOI": "010",  # Department of the Interior
        "DOJ": "015",  # Department of Justice
        "DOL": "016",  # Department of Labor
        "DOS": "019",  # Department of State
        "DOT": "069",  # Department of Transportation
        "TREAS": "020",  # Department of the Treasury
        "VA": "036",  # Department of Veterans Affairs
        # Independent Agencies
        "EPA": "020",  # Environmental Protection Agency
        "GSA": "023",  # General Services Administration
        "NASA": "026",  # National Aeronautics and Space Administration
        "NARA": "027",  # National Archives and Records Administration
        "NSF": "049",  # National Science Foundation
        "OPM": "024",  # Office of Personnel Management
        "SBA": "073",  # Small Business Administration
        "SSA": "028",  # Social Security Administration
        "USAID": "072",  # US Agency for International Development
        "NRC": "031",  # Nuclear Regulatory Commission
        "SEC": "050",  # Securities and Exchange Commission
        "USPS": "018",  # United States Postal Service
        "FCC": "027",  # Federal Communications Commission
        "FDIC": "086",  # Federal Deposit Insurance Corporation
        "FTC": "029",  # Federal Trade Commission
    }
    return AGENCY_BUREAU_CODES.get(agency.upper())


def codejson_release_to_dcat(release: dict, agency: str, organization_id: str) -> dict:
    """
    Transform a single code.json release into a DCAT-US 3.0 dataset.

    Args:
        release: Single release object from code.json
        agency: Agency acronym from code.json top-level
        organization_id: Harvester organization ID

    Returns:
        DCAT-US 3.0 compliant dataset dict
    """
    # Basic dataset structure
    dcat_dataset = {
        "@type": "dcat:Dataset",
        "identifier": release["repositoryURL"],
        "title": release["name"],
        "description": release.get("description", "No description provided"),
        "accessLevel": "public",
        "keyword": release.get("tags", []),
        "theme": release.get("languages", []),
        "landingPage": release.get("homepageURL", release["repositoryURL"]),
    }

    # Publisher
    publisher_name = release.get("organization", agency)
    dcat_dataset["publisher"] = {
        "@type": "org:Organization",
        "name": publisher_name,
    }
    if release.get("organization"):
        # If sub-organization exists, add parent agency
        dcat_dataset["publisher"]["subOrganizationOf"] = {
            "@type": "org:Organization",
            "name": agency,
        }

    # Contact point
    contact = release.get("contact", {})
    dcat_dataset["contactPoint"] = {
        "@type": "vcard:Contact",
        "fn": contact.get("URL", "Unknown"),
        "hasEmail": format_email(contact.get("email")),
    }

    # Dates
    dates = release.get("date", {})
    if dates.get("created"):
        dcat_dataset["issued"] = dates["created"]
    if dates.get("lastModified"):
        dcat_dataset["modified"] = dates["lastModified"]

    # License
    permissions = release.get("permissions", {})
    licenses = permissions.get("licenses", [])
    if licenses:
        dcat_dataset["license"] = licenses[0].get("URL") or licenses[0].get("name")

    # Distribution (repository as distribution)
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

    # Preserve code.json specific fields
    dcat_dataset["codejson"] = {
        "status": release.get("status"),
        "laborHours": release.get("laborHours"),
        "vcs": vcs,
        "usageType": permissions.get("usageType"),
    }

    # Add bureau code if available
    bureau_code = get_bureau_code_for_agency(agency)
    if bureau_code:
        dcat_dataset["bureauCode"] = [bureau_code]

    return dcat_dataset
