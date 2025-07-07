import logging
import random

import click
import requests

from deepdiff import DeepDiff

from .utils import OutputBase, CATALOG_PROD_BASE_URL, CATALOG_NEXT_BASE_URL

logger = logging.getLogger(__name__)


class Datasets(OutputBase):

    def __init__(self, base_url: str, other_datasets: list = None, **kwargs):
        super().__init__(**kwargs)
        self.base_url = base_url
        self.other_datasets = other_datasets

        self.datasets = []
        self.missing_datasets = []

        # the idea is to get 25 out of 1000 attempts
        self.sample_size = 25

        self.seed_val = random.randint(1, 100)
        self.write_to_file("dataset_seed_value.txt", str(self.seed_val))
        random.seed(self.seed_val)

        self.get_num_datasets()

        self.start = random.randint(1, int(self.num_datasets - self.sample_size))
        self.package_url = (
            f"{self.base_url}/api/action/package_search"
            f"?start={self.start}&rows={self.sample_size}&sort=id%20asc"
        )

    @staticmethod
    def _get_extra_named(item_dict, name):
        """Get the value of an extra by name.

        Extras are a list of dicts inside of org_dict each with a "key" and a
        corresponding "value". This gets the value for a corresponding key
        if it exists. If the key occurs multiple times we return the first value.

        If the key doesn't exist, we return None.
        """
        values_iter = iter(
            extra["value"]
            for extra in item_dict.get("extras", [])
            if extra["key"] == name
        )
        try:
            return next(values_iter)
        except StopIteration:
            return None

    def get_num_datasets(self):
        res = requests.get(f"{self.base_url}/api/action/package_search")
        res.raise_for_status()
        self.num_datasets = res.json()["result"]["count"]

    def fetch_matching_dataset(self, other):
        """Find and return a dataset that "matches" other from self.base_url.

        Returns None if no matching dataset is found.
        """
        url = f'{self.base_url}/api/action/package_search?fq=name:"{other["name"]}"'
        res = requests.get(url)
        if res.ok:
            data = res.json()["result"]["results"]
            if len(data) == 0:
                return None
            else:
                return data[0]
        else:
            return None

    def get_datasets(self):
        if not self.other_datasets:
            # we have to go get the datasets since they weren't specified
            res = requests.get(self.package_url)
            res.raise_for_status()
            self.datasets = res.json()["result"]["results"]
        else:
            # datasets from other catalog were specified
            with click.progressbar(self.other_datasets) as bar:
                for other in bar:
                    matching = self.fetch_matching_dataset(other)
                    if matching:
                        self.datasets.append(matching)
                    else:
                        # could not find a matching dataset
                        self.datasets.append(None)  # maintain ordering
                        self.missing_datasets.append(other)
                        click.echo(f'dataset "{other["name"]}" not found on production')

    def write_missing_datasets(self):
        data = [["name"]]
        for d in self.missing_datasets:
            data.append([d["name"]])
        self.write_to_csv("prod_missing_dataset.csv", data)


def munge_name(name: str) -> str:
    name = "_".join(map(str.lower, name.split()))
    name = name.replace("/", "")
    return name


def compare_datasets(output_dir):
    logger.info("Getting datasets from Catalog-next")
    catalog_next_datasets = Datasets(CATALOG_NEXT_BASE_URL, output_dir=output_dir)
    catalog_next_datasets.get_datasets()

    logger.info("Getting matching datasets from Catalog")
    catalog_datasets = Datasets(
        CATALOG_PROD_BASE_URL,
        other_datasets=catalog_next_datasets.datasets,
        output_dir=output_dir,
    )
    catalog_datasets.get_datasets()

    for catalog_dataset, catalog_next_dataset in zip(
        catalog_datasets.datasets, catalog_next_datasets.datasets
    ):
        if not catalog_dataset:
            # no matching dataset in catalog, handled by missing
            continue
        diff = DeepDiff(catalog_dataset, catalog_next_dataset)

        name = munge_name(catalog_dataset["name"])
        file_path = f"{name}_diff.json"

        catalog_datasets.write_to_file(file_path, diff.to_json())

    catalog_datasets.write_missing_datasets()
