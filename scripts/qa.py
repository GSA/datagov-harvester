import requests
from functools import wraps
from time import time
import csv
import random
from deepdiff import DeepDiff
from sqlalchemy.orm import scoped_session, sessionmaker
from harvester import HarvesterDBInterface
from sqlalchemy import create_engine

CATALOG_PROD_BASE_URL = "https://catalog.data.gov"
CATALOG_NEXT_BASE_URL = "https://catalog-next-dev-datagov.app.cloud.gov"


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


class HarvestSources:
    def __init__(self, source_type: str, db_interface: HarvesterDBInterface = None):
        self.source_type = source_type  # catalog-next or catalog
        self.db_interface = db_interface

        self.sources = {}

        # add any other data we want to compare
        self.source_template = {
            "organization": None,
            "name": None,
            "url": None,
            "frequency": None,
            "schema_type": None,  # "source_type"
        }

        self.rows = 1000

        self.catalog_harvest_sources_url = (
            "https://catalog.data.gov/api/action/package_search"
            f"?fq=(dataset_type:harvest)&rows={self.rows}"
        )

    def get_harvest_sources(self):
        if self.source_type == "catalog":
            res = requests.get(self.catalog_harvest_sources_url)
            if res.ok:
                self.sources = res.json()["result"]
        else:
            a = 10


class Datasets:
    def __init__(self, source_type: str, dataset_titles: list = None):
        self.source_type = source_type
        self.dataset_titles = dataset_titles

        self.num_datasets = 0

        self.datasets = []
        self.sample_size = 1000
        self.min_sample_size = 25

        self.seed_val = random.randint(1, 100)
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

    def get_dataset_titles(self):
        self.dataset_titles = [dset["title"] for dset in self.datasets]

    def get_datasets(self):
        if self.source_type == "catalog-next":
            res = requests.get(self.package_url)
            if res.ok:
                self.datasets = res.json()["result"]["results"]
        else:
            for title in self.dataset_titles:
                url = f'{self.base_url}/api/action/package_search?fq=title:"{title}"'
                res = requests.get(url)
                if res.ok:
                    data = res.json()["result"]["results"]
                    if len(data) == 0:
                        print(f'dataset "{title}" not found on production. exiting')
                    else:
                        if len(self.datasets) == self.min_sample_size:
                            break
                        self.datasets.append(data[0])


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
            org_data["image_url"] == org_data_next["logo"],
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

    summary_csv = write_to_csv("org_summary_compare.csv", summary_output)
    attribute_csv = write_to_csv("org_attr_compare.csv", attribute_output)

    return summary_csv, attribute_csv, is_same


def compare_harvest_sources(db_interface):
    catalog_harvest_sources = HarvestSources("catalog")
    catalog_harvest_sources.get_harvest_sources()

    catalog_next_harvest_sources = HarvestSources("catalog-next", db_interface)
    catalog_next_harvest_sources.get_harvest_sources()


def compare_datasets():
    output = {}

    catalog_next_datasets = Datasets("catalog-next")
    catalog_next_datasets.get_datasets()
    catalog_next_datasets.get_dataset_titles()

    catalog_datasets = Datasets("catalog", catalog_next_datasets.dataset_titles)
    catalog_datasets.get_datasets()

    for catalog_dataset in catalog_datasets.datasets:
        idx = catalog_next_datasets.dataset_titles.index(catalog_dataset["title"])

        catalog_next_dataset = catalog_next_datasets.datasets[idx]

        diff = DeepDiff(catalog_dataset, catalog_next_dataset)

        output[catalog_dataset["title"]] = diff

    return output


@timing
def main(job_type: str):
    try:
        if job_type == "all":
            compare_organizations()
            compare_harvest_sources()
            compare_datasets()
        if job_type == "organization":
            compare_organizations()
        if job_type == "harvest_source":
            db_uri = None
            engine = create_engine(db_uri)
            session_factory = sessionmaker(bind=engine, autoflush=True)
            session = scoped_session(session_factory)
            db_interface = HarvesterDBInterface(session=session)

            compare_harvest_sources(db_interface)

        if job_type == "dataset":
            compare_datasets()
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main("harvest_source")
