import requests
from functools import wraps
from time import time
import csv


## helpers functions
def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        print(f"Function {f.__name__} took {te - ts:2.4f} seconds")
        return result

    return wrap


def write_to_csv(file_path: str, data: list) -> str:
    with open(file_path, "w") as file:
        writer = csv.writer(file)
        writer.writerows(data)

    return file_path


## classes
class Organizations:
    def __init__(self, source_type: str):
        self.source_type = source_type  # catalog-next or catalog

        self.orgs = {}
        self.org_template = {"type": None}  # add any other data we want to compare

        if source_type == "catalog":
            self.base_url = "https://catalog.data.gov"
        else:
            self.base_url = "https://catalog-next-dev-datagov.app.cloud.gov"

        self.org_list_url = f"{self.base_url}/api/action/organization_list"
        self.org_url = f"{self.base_url}/api/action/organization_show"

        self.get_organization()

    def get_organization(self, org_name: str):
        res = requests.post(self.org_url, data={"id": org_name})
        if res.ok:
            return res.json()["result"]

    def get_organization_type(self, org_data):
        for extra in org_data["extras"]:
            if extra["key"] == "organization_type":
                return extra["value"]

    def get_organizations(self):
        res = requests.get(self.org_list_url)
        if res.ok:
            data = res.json()
            self.orgs = {
                org_name: dict(self.org_template) for org_name in data["result"]
            }

    def prepare_organizations(self):
        for org_name in self.orgs:
            org_data = self.get_organization(org_name)

            # get whatever attributes you want here
            self.orgs[org_name]["type"] = self.get_organization_type(org_data)


class HarvestSources:
    def __init__(self):
        #
        pass

    # harvest source data
    def get_harvest_source_count():
        url = "https://catalog.data.gov/api/action/package_search?fq=(dataset_type:harvest)"
        res = requests.get(url)
        if res.ok:
            return res.json()["result"]["count"]


class Datasets:
    def __init__(self):
        self.sample_size = 50

    # dataset data
    def get_harvest_source_dataset_count():
        return


## main comparison functions
def compare_organizations():
    catalog_orgs = Organizations("catalog")
    catalog_orgs.prepare_organizations()

    catalog_next_orgs = Organizations("catalog-next")
    catalog_next_orgs.prepare_organizations()

    attribute_output = [["catalog_name", "on_catalog-next", "same_org_type"]]

    for org_name, org_data in catalog_orgs.orgs.items():
        org_data_next = catalog_next_orgs.orgs.get(org_name, catalog_orgs.org_template)

        attribute_output.append(
            [
                org_name,
                org_name in catalog_next_orgs.orgs,
                org_data["type"] == org_data_next["type"],
            ]
        )

    summary_output = [
        ["catalog_count", "catalog-next_count"],
        [len(catalog_orgs.orgs), len(catalog_next_orgs.orgs)],
    ]

    summary_csv = write_to_csv("org_summary_compare.csv", summary_output)
    attribute_csv = write_to_csv("org_attr_compare.csv", attribute_output)

    return summary_csv, attribute_csv


def compare_harvest_sources():
    pass


def compare_datasets():
    pass


@timing
def main(job_type: str):
    if job_type == "all":
        compare_organizations()
        compare_harvest_sources()
        compare_datasets()
    if job_type == "organization":
        compare_organizations()
    if job_type == "harvest_source":
        compare_harvest_sources()
    if job_type == "dataset":
        compare_datasets()


if __name__ == "__main__":
    main("organization")
