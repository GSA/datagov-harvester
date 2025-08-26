import click

from . import session
from .utils import CATALOG_NEXT_BASE_URL, CATALOG_PROD_BASE_URL, OutputBase

"""Organization collection for QA."""


class Organizations(OutputBase):
    def __init__(self, base_url, **kwargs):
        super().__init__(**kwargs)
        self.base_url = base_url

        # add any other data we want to compare
        self.org_template = {"type": None, "logo": None, "package_count": None}

        self.org_list_url = f"{self.base_url}/api/action/organization_list"
        self.org_url = f"{self.base_url}/api/action/organization_show"

        click.echo(f"Getting organization information from {self.base_url}")
        self.orgs = self.get_organization_list()
        self.get_all_organization_details()

    def get_organization_details(self, org_name: str):
        res = session.post(self.org_url, data={"id": org_name})
        if res.ok:
            return res.json()["result"]

    def get_organization_type(self, org_data):
        for extra in org_data["extras"]:
            if extra["key"] == "organization_type":
                return extra["value"]

    def get_organization_list(self):
        res = session.get(self.org_list_url)
        res.raise_for_status()

        data = res.json()
        return {org_name: dict(self.org_template) for org_name in data["result"]}

    def get_organization_counts(self, org_name: str):
        fq = "collection_package_id:*%20OR%20"
        if "beta" in self.base_url:
            fq = "include_collection:true"

        res = session.get(
            (
                f"{self.base_url}/api/action/package_search"
                f"?q=organization:{org_name}&fq={fq}"
            )
        )
        if res.ok and res.json().get("result"):
            return res.json()["result"]["count"]

    def get_all_organization_details(self):
        with click.progressbar(self.orgs) as orgs:
            for org_name in orgs:
                org_data = self.get_organization_details(org_name)
                count_data = self.get_organization_counts(org_name)

                # get whatever attributes you want here
                self.orgs[org_name]["type"] = self.get_organization_type(org_data)
                self.orgs[org_name]["logo"] = org_data["image_url"]
                self.orgs[org_name]["name"] = org_data["name"]
                self.orgs[org_name]["id"] = org_data["id"]
                self.orgs[org_name]["package_count"] = count_data


def compare_organizations(output_dir):
    click.echo("Comparing organizations")
    catalog_orgs = Organizations(
        CATALOG_PROD_BASE_URL, output_dir=output_dir / "organizations"
    )
    catalog_next_orgs = Organizations(
        CATALOG_NEXT_BASE_URL, output_dir=output_dir / "organizations"
    )

    attribute_fields = [
        "catalog_name",
        "on_catalog-next",
        "same_org_type",
        "same_logo",
        "catalog_package_count",
        "catalog_next_package_count",
    ]
    attribute_output = [attribute_fields]

    for org_name, org_data in catalog_orgs.orgs.items():
        org_data_next = catalog_next_orgs.orgs.get(org_name, catalog_orgs.org_template)

        compare_data = [
            org_data["type"] == org_data_next["type"],
            org_data["logo"] == org_data_next["logo"],
            org_data["package_count"],
            org_data_next["package_count"],
        ]

        attribute_output.append(
            [org_name, org_name in catalog_next_orgs.orgs, *compare_data]
        )

    summary_fields = ["catalog_count", "catalog-next_count"]
    summary_output = [
        summary_fields,
        [len(catalog_orgs.orgs), len(catalog_next_orgs.orgs)],
    ]

    summary_csv = catalog_orgs.write_to_csv("org_summary_compare.csv", summary_output)
    attribute_csv = catalog_orgs.write_to_csv("org_attr_compare.csv", attribute_output)

    return summary_csv, attribute_csv
