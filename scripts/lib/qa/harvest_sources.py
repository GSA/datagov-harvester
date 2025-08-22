import click

from . import session
from .utils import CATALOG_NEXT_BASE_URL, CATALOG_PROD_BASE_URL, OutputBase


class HarvestSources(OutputBase):
    def __init__(self, source_type: str, **kwargs):
        super().__init__(**kwargs)
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
            self.harvest_sources_dset_count_url = (
                f"{CATALOG_PROD_BASE_URL}/api/action/package_search"
                f"?facet.field=%5B%22harvest_source_title%22%5D&facet.limit=-1"
            )
        else:
            self.harvest_sources_url = (
                "https://datagov-harvest.app.cloud.gov/harvest_sources/?paginate=false"
            )
            self.harvest_sources_dset_count_url = (
                f"{CATALOG_NEXT_BASE_URL}/api/action/package_search"
                f"?facet.field=%5B%22harvest_source_title%22%5D&facet.limit=-1"
            )

    def get_harvest_sources(self):
        res = session.get(self.harvest_sources_url)
        if res.ok:
            if self.source_type == "catalog":
                self.sources = res.json()["result"]["results"]
            else:
                self.sources = res.json()
        self.sources = {source["name"]: source for source in self.sources}

    def map_names_and_titles(self, harvest_counts):
        mapping = session.get((
            f"{CATALOG_PROD_BASE_URL}/api/action/package_search"
            f"?fq=dataset_type:harvest&fl=title,name&rows=1000"
        )).json()["result"]["results"]
        mapped = {}
        for item in mapping:
            mapped[item["title"]] = item["name"]
        mapped_harvest_counts = {}
        for k in harvest_counts:
            try:
                mapped_harvest_counts[mapped[k]] = harvest_counts[k]
            except KeyError:
                mapped_harvest_counts[k] = harvest_counts[k]
                continue
        return mapped_harvest_counts

    def get_num_datasets(self):
        # harvest sources with no datasets aren't returned from the solr facet
        res = session.get(self.harvest_sources_dset_count_url)
        if res.ok:
            mapped_res = self.map_names_and_titles(res.json()["result"]["facets"]["harvest_source_title"])
            self.titles = {
                title: count
                for title, count in mapped_res.items()
            }


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


def compare_dataset_counts(catalog_sources: dict, next_source: dict) -> list:
    output = []

    for name, count in catalog_sources.items():
        output.append([name, count, next_source.get(name, 0)])

    return sorted(output, key=lambda r: r[1], reverse=True)


def compare_harvest_sources(output_dir):
    click.echo("Comparing harvest sources")
    output_dir = output_dir / "harvest_sources"

    catalog_harvest_sources = HarvestSources("catalog", output_dir=output_dir)
    catalog_harvest_sources.get_harvest_sources()
    catalog_harvest_sources.get_num_datasets()

    catalog_next_harvest_sources = HarvestSources("catalog-next", output_dir=output_dir)
    catalog_next_harvest_sources.get_harvest_sources()
    catalog_next_harvest_sources.get_num_datasets()

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
            hs_data["organization"]["id"]
            == harvest_source_data_next["organization_id"],
            hs_data["name"] == harvest_source_data_next["name"],
            hs_data["url"] == harvest_source_data_next["url"],
            hs_data["frequency"].lower() == harvest_source_data_next["frequency"],
            compare_schema_types(
                harvest_source_data_next["schema_type"], hs_data["source_type"]
            ),
        ]

        attribute_output.append(
            [hs_name, hs_name in catalog_harvest_sources.sources, *compare_data]
        )

    dataset_counts = compare_dataset_counts(
        catalog_harvest_sources.titles, catalog_next_harvest_sources.titles
    )

    dataset_count_summary_fields = [
        "catalog_harvest_source_name",
        "catalog_harvest_source_dataset_count",
        "catalog_next_harvest_source_dataset_count",
    ]
    dataset_count_output = [dataset_count_summary_fields] + dataset_counts

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

    dataset_count_csv = catalog_harvest_sources.write_to_csv(
        "harvest_source_dataset_count.csv",
        dataset_count_output,
    )

    summary_csv = catalog_harvest_sources.write_to_csv(
        "harvest_source_summary_compare.csv", summary_output
    )

    attribute_csv = catalog_harvest_sources.write_to_csv(
        "harvest_source_attr_compare.csv", attribute_output
    )

    return dataset_count_csv, summary_csv, attribute_csv
