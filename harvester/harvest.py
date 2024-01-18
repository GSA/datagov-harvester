from .utils import dataset_to_hash, sort_dataset, open_json

from dataclasses import dataclass, field
import os
from pathlib import Path
import logging
import xml.etree.ElementTree as ET
import re

import ckanapi
from jsonschema import Draft202012Validator
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

ckan = ckanapi.RemoteCKAN(os.getenv("CKAN_URL"), apikey=os.getenv("CKAN_API_TOKEN_DEV"))

ROOT_DIR = Path(__file__).parents[1]


@dataclass
class HarvestSource:
    """Class for Harvest Sources"""

    # making these fields read-only
    # (these values can be set during initialization though)
    _title: str
    _url: str
    _extract_type: str  # dcatus vs waf
    _waf_config: dict = field(default_factory=lambda: {})
    _extra_source_name: str = "harvest_source_name"  # TODO: update this
    _dataset_schema: dict = field(
        default_factory=lambda: open_json(
            ROOT_DIR / "data" / "dcatus" / "schemas" / "dataset.json"
        )
    )

    # not read-only because these values are added after initialization
    # making them a "property" would require a setter which, because they are dicts,
    # means creating a custom dict class which overloads the __set_item__ method
    # worth it? not sure...
    records: dict = field(default_factory=lambda: {})
    ckan_records: dict = field(default_factory=lambda: {})

    def __post_init__(self) -> None:
        # putting this in the post init to avoid an
        # undefined error with the string formatted vars
        self._ckan_query: dict = {
            "q": f'{self._extra_source_name}:"{self._title}"',
            "fl": [
                f"extras_{self._extra_source_name}",
                "extras_dcat_metadata",
                "extras_identifier",
                # TODO: add last_modified for waf
            ],
        }

    @property
    def title(self) -> str:
        return self._title

    @property
    def url(self) -> str:
        return self._url

    @property
    def extract_type(self) -> str:
        return self._extract_type

    @property
    def waf_config(self) -> dict:
        return self._waf_config

    @property
    def extra_source_name(self) -> str:
        return self._extra_source_name

    @property
    def dataset_schema(self) -> dict:
        return self._dataset_schema

    @property
    def ckan_query(self) -> dict:
        return self._ckan_query

    def get_title_from_fgdc(self, xml_str: str) -> str:
        tree = ET.ElementTree(ET.fromstring(xml_str))
        return tree.find(".//title").text

    def harvest_to_id_hash(self, records: list[dict]) -> None:
        """i = identifier; h = hash"""
        for record in records:
            if self.extract_type == "dcatus":
                i, h = record["identifier"], dataset_to_hash(sort_dataset(record))
            if self.extract_type == "waf":
                i, h = self.get_title_from_fgdc(record["content"]), dataset_to_hash(
                    record["content"].decode(
                        "utf-8"
                    )  # TODO: is this the correct encoding?
                )
            self.records[i] = Record(
                self,
                i,
                record,
                h,
            )

    def ckan_to_id_hash(self, results: list[dict]) -> None:
        for record in results:
            self.ckan_records[record["identifier"]] = dataset_to_hash(
                eval(
                    record["dcat_metadata"], {"__builtins__": {}}
                )  # TODO: eval is a big security risk. think of a better solution.
            )

    def traverse_waf(self, url, files=[], file_ext=".xml", folder="/", filters=[]):
        """Transverses WAF
        Please add docstrings
        """
        # TODO: add exception handling
        parent = os.path.dirname(url.rstrip("/"))

        res = requests.get(url)
        if res.status_code == 200:
            soup = BeautifulSoup(res.content, "html.parser")
            anchors = soup.find_all("a", href=True)

            folders = []
            for anchor in anchors:
                if (
                    anchor["href"].endswith(folder)
                    and not parent.endswith(anchor["href"].rstrip("/"))
                    and anchor["href"] not in filters
                ):
                    folders.append(os.path.join(url, anchor["href"]))

                if anchor["href"].endswith(file_ext):
                    # TODO: just download the file here
                    # instead of storing them and returning at the end.
                    files.append(os.path.join(url, anchor["href"]))

        for folder in folders:
            self.traverse_waf(folder, files=files, filters=filters)

        return files

    def download_waf(self, files):
        """Downloads WAF
        Please add docstrings
        """
        output = []
        for file in files:
            res = requests.get(file)
            if res.status_code == 200:
                output.append({"url": file, "content": res.content})

        return output

    def compare(self) -> None:
        """Compares records"""
        logger.info("Hello from harvester.compare()")

        harvest_ids = set(self.records.keys())
        ckan_ids = set(self.ckan_records.keys())
        same_ids = harvest_ids & ckan_ids

        # since python 3.7 dicts are insertion ordered so deletions will occur first
        self.compare_data = {"delete": None, "create": None, "update": None}
        self.compare_data["delete"] = ckan_ids - harvest_ids
        self.compare_data["create"] = harvest_ids - ckan_ids
        self.compare_data["update"] = set(
            [
                i
                for i in same_ids
                if self.records[i].metadata_hash != self.ckan_records[i]
            ]
        )

    def get_ckan_records(self) -> None:
        return ckan.action.package_search(**self.ckan_query)["results"]

    def get_ckan_records_as_id_hash(self) -> None:
        self.ckan_to_id_hash(self.get_ckan_records())

    def get_harvest_records_as_id_hash(self) -> None:
        if self.extract_type == "dcatus":
            self.harvest_to_id_hash(requests.get(self.url).json()["dataset"])
        if self.extract_type == "waf":
            self.harvest_to_id_hash(
                self.download_waf(self.traverse_waf(self.url, **self.waf_config))
            )

    def get_record_changes(self) -> None:
        """determine which records needs to be updated, deleted, or created"""
        self.get_ckan_records_as_id_hash()
        self.get_harvest_records_as_id_hash()
        self.compare()

    def synchronize_records(self) -> None:
        """runs the delete, update, and create"""
        for operation, ids in self.compare_data.items():
            for i in ids:
                if operation == "delete":
                    # we don't actually create a Record instance for deletions
                    # so creating it here as a sort of acknowledgement
                    self.records[i] = Record(self, i)
                    self.records[i].delete_record()
                    continue

                record = self.records[i]
                # no longer setting operation in compare so setting it here...
                record.operation = operation

                record.validate()
                # TODO: add transformation and validation
                record.sync()

    def summarize(self) -> None:
        # TODO: this method
        # get base stats of the workflow
        # how many deleted? created? updated?
        # what about validation?
        return

    def upload_to_s3(self) -> None:
        # TODO: this method
        return


@dataclass
class Record:
    """Class for Harvest Records"""

    _harvest_source: HarvestSource
    _identifier: str
    _metadata: dict = field(default_factory=lambda: {})
    _metadata_hash: str = ""
    _operation: str = None
    _valid: bool = False
    _validation_msg: str = ""
    _status: str = ""

    ckanified_metadata: dict = field(default_factory=lambda: {})

    @property
    def harvest_source(self) -> HarvestSource:
        return self._harvest_source

    @property
    def identifier(self) -> str:
        return self._identifier

    @property
    def metadata(self) -> dict:
        return self._metadata

    @property
    def metadata_hash(self) -> str:
        return self._metadata_hash

    @property
    def operation(self):
        return self._operation

    @operation.setter
    def operation(self, value):
        if value not in ["create", "update", "delete", None]:
            raise ValueError("Unknown operation being set to record")
        self._operation = value

    @property
    def valid(self):
        return self._valid

    @valid.setter
    def valid(self, value):
        if not isinstance(value, bool):
            raise ValueError("Record validity must be expressed as a boolean")
        self._valid = value

    @property
    def validation_msg(self):
        return self._validation_msg

    @validation_msg.setter
    def validation_msg(self, value):
        if not isinstance(value, str):
            raise ValueError("status must be a string")
        self._validation_msg = value

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        if not isinstance(value, str):
            raise ValueError("status must be a string")
        self._status = value

    # ckanify methods
    def create_ckan_extra_base(self, *args) -> dict:
        keys = ["publisher_hierarchy", "resource-type", "publisher"]
        data = zip(keys, args)
        return [{"key": d[0], "value": d[1]} for d in data]

    def create_ckan_extras_additions(
        self, metadata: dict, additions: list[dict]
    ) -> list[dict]:
        extras = [
            "accessLevel",
            "bureauCode",
            "identifier",
            "modified",
            "programCode",
            "publisher",
        ]

        output = []

        for extra in extras:
            data = {"key": extra, "value": None}
            val = metadata[extra]
            if extra == "publisher":
                data["value"] = val["name"]
            else:
                if isinstance(val, list):  # TODO: confirm this is what we want.
                    val = val[0]
                data["value"] = val
            output.append(data)

        return output + additions

    def create_ckan_tags(self, keywords: list[str]) -> list:
        output = []

        for keyword in keywords:
            keyword = "-".join(keyword.split())
            output.append({"name": keyword})

        return output

    def create_ckan_publisher_hierarchy(self, pub_dict: dict, data: list = []):
        for k, v in pub_dict.items():
            if k == "name":
                data.append(v)
            if isinstance(v, dict):
                self.create_ckan_publisher_hierarchy(v, data)

        return " > ".join(data[::-1])

    def get_email_from_str(self, in_str: str) -> str:
        res = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", in_str)
        if res is not None:
            return res.group(0)

    def create_ckan_resources(self, metadata: dict) -> list[dict]:
        output = []

        if "distribution" not in metadata:
            return output

        for dist in metadata["distribution"]:
            url_key = "downloadURL" if "downloadURL" in dist else "accessURL"
            resource = {"url": dist[url_key]}
            if "mimetype" in dist:
                resource["mimetype"] = dist["mediaType"]

            output.append(resource)

        return output

    def simple_transform(self, metadata: dict) -> dict:
        output = {
            "name": "-".join(metadata["title"].lower().split()),
            "owner_org": "test",  # TODO: CHANGE THIS!
            "identifier": self.metadata["identifier"],
        }

        mapping = {
            "contactPoint": {"fn": "maintainer", "hasEmail": "maintainer_email"},
            "description": "notes",
            "title": "title",
        }

        for k, v in metadata.items():
            if k not in mapping:
                continue
            if isinstance(mapping[k], dict):
                temp = {}
                to_skip = ["@type"]
                for k2, v2 in v.items():
                    if k2 == "hasEmail":
                        v2 = self.get_email_from_str(v2)
                    if k2 in to_skip:
                        continue
                    temp[mapping[k][k2]] = v2
                output = {**output, **temp}
            else:
                output[mapping[k]] = v

        return output

    def create_defaults(self) -> dict:
        return {
            "author": None,
            "author_email": None,
        }

    def ckanify_dcatus(self) -> None:
        output = self.simple_transform(self.metadata)

        resources = self.create_ckan_resources(self.metadata)
        tags = self.create_ckan_tags(self.metadata["keyword"])
        pubisher_hierarchy = self.create_ckan_publisher_hierarchy(
            self.metadata["publisher"], []
        )

        extras_base = self.create_ckan_extra_base(
            pubisher_hierarchy, "Dataset", self.metadata["publisher"]["name"]
        )
        extras = self.create_ckan_extras_additions(self.metadata, extras_base)

        defaults = self.create_defaults()

        output["resources"] = resources
        output["tags"] = tags

        output["extras"] = extras_base
        output["extras"] += extras
        output["extras"] += [
            {
                "key": "dcat_metadata",
                "value": str(sort_dataset(self.metadata)),
            }
        ]

        output["extras"] += [
            {
                "key": self.harvest_source.extra_source_name,
                "value": self.harvest_source.title,
            }
        ]

        self.ckanified_metadata = {**output, **defaults}

    def validate(self) -> None:
        validator = Draft202012Validator(self.harvest_source.dataset_schema)
        try:
            validator.validate(self.metadata)
            self.valid = True
        except Exception as e:
            self.validation_msg = str(e)  # TODO: verify this is what we want

    # def transform(self, metadata: dict):
    #     """Transforms records"""
    #     logger.info("Hello from harvester.transform()")

    #     url = os.getenv("MDTRANSLATOR_URL")
    #     res = requests.post(url, metadata)

    #     if res.status_code == 200:
    #         data = json.loads(res.content.decode("utf-8"))
    #         transform_obj["transformed_data"] = data["writerOutput"]

    #     return transform_obj

    # the reason for abstracting these calls is because I couldn't find a
    # way to properly patch the ckan.action calls in my tests
    def create_record(self):
        res = ckan.action.package_create(**self.ckanified_metadata)
        self.status = "created"
        return res

    def update_record(self):
        res = ckan.action.package_update(**self.ckanified_metadata)
        self.status = "updated"
        return res

    def delete_record(self):
        # purge returns nothing
        ckan.action.dataset_purge(**{"name": self.identifier})
        self.status = "deleted"

    def sync(self) -> None:
        # TODO: do we want to do something with the validity of the record?

        self.ckanify_dcatus()

        if self.operation == "create":
            self.create_record()
        if self.operation == "update":
            self.update_record()


if __name__ == "__main__":
    config = {
        "_title": "test_harvest_source_name",
        "_url": "http://localhost/dcatus/dcatus_compare.json",
        "_extract_type": "dcatus",
    }

    harvest_source = HarvestSource(**config)
    harvest_source.get_record_changes()
    harvest_source.synchronize_records()
