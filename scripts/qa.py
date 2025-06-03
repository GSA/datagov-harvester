import argparse
import csv
import json
import logging
import os
import random
import shutil
import sys
from enum import Enum
from functools import wraps
from pathlib import Path
from time import time

import requests
from deepdiff import DeepDiff

logger = logging.getLogger(__name__)

CATALOG_PROD_BASE_URL = "https://catalog.data.gov"
CATALOG_NEXT_BASE_URL = "https://catalog-next-dev-datagov.app.cloud.gov"
SCRIPT_DIR = Path(__file__).parents[0]
OUTPUT_DIR = SCRIPT_DIR / "qa_output"

# prepare output directory
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)  # deletes dir and all contents
os.mkdir(OUTPUT_DIR)


## helpers functions
def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        ts = time()
        result = f(*args, **kw)
        te = time()
        logger.info(f"Function {f.__name__} took {te - ts:2.4f} seconds")
        return result

    return wrap


def write_to_csv(file_path: str, data: list) -> str:
    with open(file_path, "w") as file:
        writer = csv.writer(file)
        writer.writerows(data)

    return file_path


def write_to_json(file_path: str, data: dict) -> str:
    with open(file_path, "w") as file:
        json.dump(data, file)
    return file_path


def write_to_file(file_path: str, data: list) -> str:
    with open(file_path, "w") as file:
        file.writelines(data)
    return file_path


def munge_name(name: str) -> str:
    name = "_".join(map(str.lower, name.split()))
    name = name.replace("/", "")
    return name


## classes
class Job(Enum):
    ALL = "all"
    ORGANIZATION = "organization"
    HARVEST_SOURCE = "harvest_source"
    DATASET = "dataset"

    def __str__(self):
        return self.value


class Organizations:
    def __init__(self, source_type: str):
        self.source_type = source_type  # catalog-next or catalog

        self.orgs = {}

        # add any other data we want to compare
        self.org_template = {"type": None, "logo": None}

        if source_type == "catalog":
            self.base_url = CATALOG_PROD_BASE_URL
        else:
            self.base_url = CATALOG_NEXT_BASE_URL

        self.org_list_url = f"{self.base_url}/api/action/organization_list"
        self.org_url = f"{self.base_url}/api/action/organization_show"

        self.get_organizations()

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
            self.orgs[org_name]["logo"] = org_data["image_url"]
            self.orgs[org_name]["name"] = org_data["name"]
            self.orgs[org_name]["id"] = org_data["id"]


class HarvestSources:
    def __init__(self, source_type: str):
        self.source_type = source_type  # catalog-next or catalog

        self.sources = {}

        # add any other data we want to compare
        self.source_template = {
            "organization_id": None,
            "name": None,
            "url": None,
            "frequency": None,
            "schema_type": "",  # "source_type"
        }

        self.rows = 1000

        if self.source_type == "catalog":
            self.harvest_sources_url = (
                f"{CATALOG_PROD_BASE_URL}/api/action/package_search"
                f"?fq=(dataset_type:harvest)&rows={self.rows}"
            )
        else:
            self.harvest_sources_url = (
                "https://datagov-harvest-admin-dev.app.cloud.gov"
                "/harvest_sources/?paginate=false"
            )

    def get_harvest_sources(self):
        res = requests.get(self.harvest_sources_url)
        if res.ok:
            if self.source_type == "catalog":
                self.sources = res.json()["result"]["results"]
            else:
                self.sources = res.json()
        self.sources = {source["name"]: source for source in self.sources}


class Datasets:
    def __init__(self, source_type: str, dataset_titles: list = None):
        self.source_type = source_type
        self.dataset_titles = dataset_titles

        self.num_datasets = 0

        self.datasets = []
        self.missing_datasets = []

        # the idea is to get 25 out of 1000 attempts
        self.sample_size = 1000
        self.min_sample_size = 25

        self.seed_val = random.randint(1, 100)

        write_to_file(
            os.path.join(OUTPUT_DIR, "dataset_seed_value.txt"), [str(self.seed_val)]
        )

        random.seed(self.seed_val)

        if source_type == "catalog":
            self.base_url = CATALOG_PROD_BASE_URL
        else:
            self.base_url = CATALOG_NEXT_BASE_URL

        self.package_search_url = f"{self.base_url}/api/action/package_search"

        self.get_num_datasets()

        self.start = random.randint(1, int(self.num_datasets - self.sample_size))

        self.package_url = (
            f"{self.base_url}/api/action/package_search"
            f"?start={self.start}&rows={self.sample_size}&sort=id%20asc"
        )

    def get_num_datasets(self):
        res = requests.get(self.package_search_url)
        if res.ok:
            self.num_datasets = res.json()["result"]["count"]
        else:
            raise Exception

    def get_dataset_names(self):
        self.dataset_titles = [dset["name"] for dset in self.datasets]

    def get_datasets(self):
        if self.source_type == "catalog-next":
            res = requests.get(self.package_url)
            if res.ok:
                self.datasets = res.json()["result"]["results"]
        else:
            for name in self.dataset_titles:
                if len(self.datasets) == self.min_sample_size:
                    break
                url = f'{self.base_url}/api/action/package_search?fq=name:"{name}"'
                res = requests.get(url)
                if res.ok:
                    data = res.json()["result"]["results"]
                    if len(data) == 0:
                        self.missing_datasets.append(name)
                        logger.warning(f'dataset "{name}" not found on production')
                    else:
                        self.datasets.append(data[0])

    def write_missing_dataset(self):
        data = [["name"]]
        for d in self.missing_datasets:
            data.append([d])
        write_to_csv(os.path.join(OUTPUT_DIR, "prod_missing_dataset.csv"), data)


## main comparison functions
def compare_organizations():
    catalog_orgs = Organizations("catalog")
    catalog_orgs.prepare_organizations()

    catalog_next_orgs = Organizations("catalog-next")
    catalog_next_orgs.prepare_organizations()

    is_same = True

    attribute_fields = ["catalog_name", "on_catalog-next", "same_org_type", "same_logo"]
    attribute_output = [attribute_fields]

    for org_name, org_data in catalog_orgs.orgs.items():
        org_data_next = catalog_next_orgs.orgs.get(org_name, catalog_orgs.org_template)

        compare_data = [
            org_data["type"] == org_data_next["type"],
            org_data["logo"] == org_data_next["logo"],
        ]

        if any(data is False for data in compare_data):
            is_same = False

        attribute_output.append(
            [org_name, org_name in catalog_next_orgs.orgs, *compare_data]
        )

    if len(catalog_orgs.orgs) != len(catalog_next_orgs.orgs):
        is_same = False

    summary_fields = ["catalog_count", "catalog-next_count"]
    summary_output = [
        summary_fields,
        [len(catalog_orgs.orgs), len(catalog_next_orgs.orgs)],
    ]

    summary_csv = write_to_csv(
        os.path.join(OUTPUT_DIR, "org_summary_compare.csv"), summary_output
    )
    attribute_csv = write_to_csv(
        os.path.join(OUTPUT_DIR, "org_attr_compare.csv"), attribute_output
    )

    return summary_csv, attribute_csv, is_same


def compare_harvest_sources():
    def compare_schema_types(schema_next: str, schema_prod: str) -> bool:
        if schema_next.startswith("dcatus") and schema_prod in [
            "datajson",
            "single-doc",
        ]:
            return True

        if schema_next.startswith("iso") and schema_prod in [
            "waf",
            "waf-collection",
            "single-doc",
        ]:
            return True

        if schema_prod in ["csw", "arcgis", "single-doc", "geoportal"]:
            return None

        return False

    def get_org_name(organizations: Organizations, org_id: str) -> dict:
        for org_name, org_data in organizations.items():
            if org_data["id"] == org_id:
                return org_data["name"]

    catalog_harvest_sources = HarvestSources("catalog")
    catalog_harvest_sources.get_harvest_sources()

    catalog_next_harvest_sources = HarvestSources("catalog-next")
    catalog_next_harvest_sources.get_harvest_sources()

    catalog_orgs = Organizations("catalog")
    catalog_orgs.prepare_organizations()

    catalog_next_orgs = Organizations("catalog-next")
    catalog_next_orgs.prepare_organizations()

    is_same = True

    attribute_fields = [
        "catalog_harvest_source_name",
        "in_catalog-next",
        "same_org",
        "same_name",
        "same_url",
        "same_frequency",
        "same_schema_type",
    ]
    attribute_output = [attribute_fields]

    for hs_name, hs_data in catalog_harvest_sources.sources.items():
        # we don't harvest these things anymore
        if hs_data["source_type"] in ["csw", "arcgis", "geoportal"]:
            continue

        harvest_source_data_next = catalog_next_harvest_sources.sources.get(
            hs_name, catalog_harvest_sources.source_template
        )

        compare_data = [
            hs_data["organization"]["name"]
            == get_org_name(
                catalog_next_orgs.orgs, harvest_source_data_next["organization_id"]
            ),
            hs_data["name"] == harvest_source_data_next["name"],
            hs_data["url"] == harvest_source_data_next["url"],
            hs_data["frequency"].lower() == harvest_source_data_next["frequency"],
            compare_schema_types(
                harvest_source_data_next["schema_type"], hs_data["source_type"]
            ),
        ]

        if any(data is False for data in compare_data):
            is_same = False

        attribute_output.append(
            [hs_name, hs_name in catalog_harvest_sources.sources, *compare_data]
        )

    summary_fields = [
        "catalog_harvest_source_count",
        "catalog-next_harvest_source_count",
    ]
    summary_output = [
        summary_fields,
        [
            len(catalog_harvest_sources.sources),
            len(catalog_next_harvest_sources.sources),
        ],
    ]

    summary_csv = write_to_csv(
        os.path.join(OUTPUT_DIR, "harvest_source_summary_compare.csv"), summary_output
    )

    attribute_csv = write_to_csv(
        os.path.join(OUTPUT_DIR, "harvest_source_attr_compare.csv"), attribute_output
    )

    return summary_csv, attribute_csv, is_same


def compare_datasets():
    catalog_next_datasets = Datasets("catalog-next")
    catalog_next_datasets.get_datasets()
    catalog_next_datasets.get_dataset_names()

    catalog_datasets = Datasets("catalog", catalog_next_datasets.dataset_titles)
    catalog_datasets.get_datasets()

    for catalog_dataset in catalog_datasets.datasets:
        idx = catalog_next_datasets.dataset_titles.index(catalog_dataset["name"])
        catalog_next_dataset = catalog_next_datasets.datasets[idx]

        diff = json.loads(DeepDiff(catalog_dataset, catalog_next_dataset).to_json())

        name = munge_name(catalog_dataset["name"])
        file_path = os.path.join(OUTPUT_DIR, f"{name}_diff.json")

        write_to_json(file_path, diff)

    catalog_datasets.write_missing_dataset()


@timing
def process_job(job_type: str):
    try:
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
    except Exception as e:
        logger.error(repr(e))


def main():
    parser = argparse.ArgumentParser(
        prog="H20 QA", description="qa organizations, harvest sources, and datasets"
    )
    parser.add_argument(
        "--job-type",
        type=Job,
        choices=list(Job),
        help="Choose a job: all, organization, harvest_source, or dataset",
        default=Job.ALL,
    )

    args = parser.parse_args(sys.argv[1:])

    process_job(str(args.job_type))


if __name__ == "__main__":
    main()
