import json
import logging
import os
import re
import sys
import traceback
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
import functools

import ckanapi
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from jsonschema import Draft202012Validator

# TODO: add relative import "." for utils
from .utils import (
    S3Handler,
    convert_set_to_list,
    dataset_to_hash,
    open_json,
    sort_dataset,
)

load_dotenv()

logging.basicConfig(
    level=logging.NOTSET,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    handlers=[
        logging.FileHandler("harvest_load.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger(__name__)

# TODD: make sure this doesn't change all requests
session = requests.Session()
session.request = functools.partial(session.request, timeout=15)

ckan = ckanapi.RemoteCKAN(
    os.getenv("CKAN_URL_STAGE"),
    apikey=os.getenv("CKAN_API_TOKEN_STAGE"),
    session=session,
)

ROOT_DIR = Path(__file__).parents[0]


@dataclass
class HarvestSource:
    """Class for Harvest Sources"""

    # making these fields read-only
    # (these values can be set during initialization though)
    _title: str
    _url: str
    _owner_org: str  # uuid
    _extract_type: str  # "datajson" or "waf-collection"
    _waf_config: dict = field(default_factory=lambda: {})
    _extra_source_name: str = "harvest_source_name"
    _dataset_schema: dict = field(
        default_factory=lambda: open_json(
            ROOT_DIR / "data" / "dcatus" / "schemas" / "dataset.json"
        )
    )
    _no_harvest_resp: bool = False
    _ckan_start: int = 0
    _ckan_row: int = 1000

    # not read-only because these values are added after initialization
    # making them a "property" would require a setter which, because they are dicts,
    # means creating a custom dict class which overloads the __set_item__ method
    # worth it? not sure...
    # since python 3.7 dicts are insertion ordered so deletions will occur first
    compare_data: dict = field(
        default_factory=lambda: {"delete": set(), "create": set(), "update": set()}
    )
    records: dict = field(default_factory=lambda: {})
    ckan_records: dict = field(default_factory=lambda: {})

    def __post_init__(self) -> None:
        # putting this in the post init to avoid an
        # undefined error with the string formatted vars
        self.ckan_query: dict = {
            "q": f'{self._extra_source_name}:"{self._title}"',
            "rows": self._ckan_row,
            "start": self._ckan_start,
            "fl": [
                f"extras_{self._extra_source_name}",
                "extras_dcat_metadata",
                "extras_identifier",
                "id",
                # TODO: add last_modified for waf
            ],
        }

    @property
    def title(self) -> str:
        return self._title

    @property
    def url(self) -> str:
        return self._url.strip()

    @property
    def owner_org(self) -> str:
        return self._owner_org

    @property
    def extract_type(self) -> str:
        # TODO: this is probably safe to assume right?
        if "json" in self._extract_type:
            return "datajson"
        if "waf" in self._extract_type:
            return "waf-collection"

    @property
    def waf_config(self) -> dict:
        return self._waf_config

    @property
    def extra_source_name(self) -> str:
        return self._extra_source_name

    @property
    def dataset_schema(self) -> dict:
        return self._dataset_schema

    @dataset_schema.deleter
    def dataset_schema(self):
        del self._dataset_schema

    @property
    def no_harvest_resp(self) -> bool:
        return self._no_harvest_resp

    @no_harvest_resp.setter
    def no_harvest_resp(self, value) -> None:
        if not isinstance(value, bool):
            raise ValueError("No harvest response field must be a boolean")
        self._no_harvest_resp = value

    @property
    def ckan_start(self) -> int:
        return self._ckan_start

    @ckan_start.setter
    def ckan_start(self, value) -> None:
        if not isinstance(value, int):
            raise ValueError("CKAN start search parameter must be an integer")
        self._ckan_start = value

    @property
    def ckan_row(self) -> int:
        return self._ckan_row

    def get_title_from_fgdc(self, xml_str: str) -> str:
        tree = ET.ElementTree(ET.fromstring(xml_str))
        return tree.find(".//title").text

    def download_dcatus(self):
        logger.info("downloading dcatus json")
        resp = requests.get(self.url)
        if resp.status_code == 200:
            return resp.json()

    def harvest_to_id_hash(self, records: list[dict]) -> None:
        logger.info("converting harvest records to id: hash")
        for record in records:
            if self.extract_type == "datajson":
                if "identifier" not in record:  # TODO: what to do?
                    continue
                identifier = record["identifier"]
                dataset_hash = dataset_to_hash(sort_dataset(record))
            if self.extract_type == "waf-collection":
                identifier = self.get_title_from_fgdc(record["content"])
                dataset_hash = dataset_to_hash(record["content"].decode("utf-8"))

            self.records[identifier] = Record(self, identifier, record, dataset_hash)

    def ckan_to_id_hash(self, results: list[dict]) -> None:
        logger.info("converting ckan records to id: hash")
        for record in results:
            dataset_hash = dataset_to_hash(
                eval(record["dcat_metadata"], {"__builtins__": {}})
            )
            # storing the ckan id in case we need to delete it later
            self.ckan_records[record["identifier"]] = [dataset_hash, record["id"]]

    def traverse_waf(self, url, files=[], file_ext=".xml", folder="/", filters=[]):
        """Transverses WAF
        Please add docstrings
        """
        # TODO: add exception handling
        parent = os.path.dirname(url.rstrip("/"))

        folders = []

        res = requests.get(url)
        if res.status_code == 200:
            soup = BeautifulSoup(res.content, "html.parser")
            anchors = soup.find_all("a", href=True)

            for anchor in anchors:
                if (
                    anchor["href"].endswith(folder)  # is the anchor link a folder?
                    and not parent.endswith(
                        anchor["href"].rstrip("/")
                    )  # it's not the parent folder right?
                    and anchor["href"]
                    not in filters  # and it's not on our exclusion list?
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
        logger.info("comparing harvest and ckan records")

        harvest_ids = set(self.records.keys())
        ckan_ids = set(self.ckan_records.keys())
        same_ids = harvest_ids & ckan_ids

        self.compare_data["delete"] = ckan_ids - harvest_ids
        self.compare_data["create"] = harvest_ids - ckan_ids

        for i in same_ids:
            if self.records[i].metadata_hash != self.ckan_records[i][0]:
                self.compare_data["update"].add(i)

    def get_ckan_records(self, results=[]) -> None:
        logger.info("querying ckan")
        res = ckan.action.package_search(**self.ckan_query)["results"]
        results += res
        if len(res) == 1000:
            self.ckan_query["start"] += self.ckan_row
            self.get_ckan_records(results)
        return results

    def get_ckan_records_as_id_hash(self) -> None:
        logger.info("retrieving and preparing ckan records")
        self.ckan_to_id_hash(self.get_ckan_records(results=[]))

    def get_harvest_records_as_id_hash(self) -> None:
        logger.info("retrieving and preparing harvest records")
        if self.extract_type == "datajson":
            download_res = self.download_dcatus()
            if download_res is None or "dataset" not in download_res:
                logger.warning(
                    "no valid response from server or lack of 'dataset' array"
                )
                self.no_harvest_resp = True
                return
            self.harvest_to_id_hash(download_res["dataset"])
        if self.extract_type == "waf-collection":
            # TODO: break out the xml catalogs as records
            # TODO: handle no response
            self.harvest_to_id_hash(
                self.download_waf(self.traverse_waf(self.url, **self.waf_config))
            )

    def get_record_changes(self) -> None:
        """determine which records needs to be updated, deleted, or created"""
        logger.info(f"getting records changes for {self.title} using {self.url}")
        try:
            self.get_ckan_records_as_id_hash()
            self.get_harvest_records_as_id_hash()
            if self.no_harvest_resp is True:
                return
            self.compare()
        except Exception as e:
            logger.error(self.title + " " + self.url + " " "\n")
            logger.error(
                "\n".join(traceback.format_exception(None, e, e.__traceback__))
            )

    def synchronize_records(self) -> None:
        """runs the delete, update, and create
        - self.compare can be empty becuase there was no harvest source response
        or there's truly nothing to process
        """
        logger.info("synchronizing records")
        for operation, ids in self.compare_data.items():
            for i in ids:
                try:
                    if operation == "delete":
                        # we don't actually create a Record instance for deletions
                        # so creating it here as a sort of acknowledgement
                        self.records[i] = Record(self, self.ckan_records[i][1])
                        self.records[i].operation = operation
                        self.records[i].delete_record()
                        continue

                    record = self.records[i]
                    # no longer setting operation in compare so setting it here...
                    record.operation = operation

                    record.validate()
                    # TODO: add transformation and validation
                    record.sync()
                except Exception as e:
                    logger.error(f"error processing '{operation}' on record {i}\n")
                    logger.error(
                        "\n".join(traceback.format_exception(None, e, e.__traceback__))
                    )

    def report(self) -> None:
        logger.info("report results")
        # log our original compare data
        logger.info("expected operations to be done")
        logger.info(
            {operation: len(ids) for operation, ids in self.compare_data.items()}
        )

        # validation count and actual results
        actual_results = {"deleted": 0, "updated": 0, "created": 0, "nothing": 0}
        validity = {"valid": 0, "invalid": 0}

        for record_id, record in self.records.items():
            # status
            actual_results[record.status] += 1
            # validity
            if record.valid:
                validity["valid"] += 1
            else:
                validity["invalid"] += 1

        # what actually happened?
        logger.info("actual operations completed")
        logger.info(actual_results)

        # what's our record validity count?
        logger.info("validity of the records")
        logger.info(validity)

    def hydrate_from_s3(self, s3_path: str) -> None:
        # TODO: this method
        return

    def prepare_for_s3_upload(self) -> dict:
        """this removes data we don't want to store in s3"""
        # TODO: confirm the minimum we need to store

        for record_id, record in self.records.copy().items():
            to_process = set().union(*self.compare_data.values())
            if record_id not in to_process:
                del self.records[record_id]
                continue
            record.harvest_source = self.title

        data = asdict(self)
        del data["ckan_records"], data["_dataset_schema"]

        return json.dumps(data, default=convert_set_to_list)

    def upload_to_s3(self, s3handler: S3Handler) -> None:
        try:
            # TODO: confirm out_path
            out_path = f"{s3handler.endpoint_url}/{self.title}/job-id/{self.title}.json"
            s3handler.put_object(self.prepare_for_s3_upload(), out_path)
            logger.info(f"saved harvest source {self.title} in s3 at {out_path}")
            return out_path
        except Exception as e:
            logger.error(f"error uploading harvest source ({self.title}) to s3 \n")
            logger.error(
                "\n".join(traceback.format_exception(None, e, e.__traceback__))
            )

        return False


@dataclass
class Record:
    """Class for Harvest Records"""

    _harvest_source: HarvestSource
    _identifier: str
    _metadata: dict = field(default_factory=lambda: {})
    _metadata_hash: str = ""
    _operation: str = None
    _valid: bool = None
    _validation_msg: str = ""
    _status: str = "nothing"

    ckanified_metadata: dict = field(default_factory=lambda: {})

    @property
    def harvest_source(self) -> HarvestSource:
        return self._harvest_source

    @harvest_source.setter
    def harvest_source(self, value) -> None:
        if not isinstance(value, (HarvestSource, str)):
            raise ValueError(
                "harvest source must be either a HarvestSource instance or a string"
            )
        self._harvest_source = value

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
    def operation(self) -> str:
        return self._operation

    @operation.setter
    def operation(self, value) -> None:
        if value not in ["create", "update", "delete", None]:
            raise ValueError("Unknown operation being set to record")
        self._operation = value

    @property
    def valid(self) -> bool:
        return self._valid

    @valid.setter
    def valid(self, value) -> None:
        if not isinstance(value, bool):
            raise ValueError("Record validity must be expressed as a boolean")
        self._valid = value

    @property
    def validation_msg(self) -> str:
        return self._validation_msg

    @validation_msg.setter
    def validation_msg(self, value) -> None:
        if not isinstance(value, str):
            raise ValueError("status must be a string")
        self._validation_msg = value

    @property
    def status(self) -> None:
        return self._status

    @status.setter
    def status(self, value) -> None:
        if not isinstance(value, str):
            raise ValueError("status must be a string")
        self._status = value

    def create_ckan_extras(self) -> list[dict]:
        extras = [
            "accessLevel",
            "bureauCode",
            "identifier",
            "modified",
            "programCode",
            "publisher",
        ]

        output = [{"key": "resource-type", "value": "Dataset"}]

        for extra in extras:
            if extra not in self.metadata:
                continue
            data = {"key": extra, "value": None}
            val = self.metadata[extra]
            if extra == "publisher":
                data["value"] = val["name"]

                output.append(
                    {
                        "key": "publisher_hierarchy",
                        "value": self.create_ckan_publisher_hierarchy(val, []),
                    }
                )

            else:
                if isinstance(val, list):  # TODO: confirm this is what we want.
                    val = val[0]
                data["value"] = val
            output.append(data)

        output.append(
            {
                "key": "dcat_metadata",
                "value": str(sort_dataset(self.metadata)),
            }
        )

        output.append(
            {
                "key": self.harvest_source.extra_source_name,
                "value": self.harvest_source.title,
            }
        )

        output.append({"key": "identifier", "value": self.identifier})

        return output

    def create_ckan_tags(self, keywords: list[str]) -> list:
        output = []

        for keyword in keywords:
            keyword = "-".join(keyword.split())
            output.append({"name": keyword})

        return output

    def create_ckan_publisher_hierarchy(self, pub_dict: dict, data: list = []) -> str:
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

        if "distribution" not in metadata or metadata["distribution"] is None:
            return output

        for dist in metadata["distribution"]:
            url_keys = ["downloadURL", "accessURL"]
            for url_key in url_keys:
                if dist.get(url_key, None) is None:
                    continue
                resource = {"url": dist[url_key]}
                if "mimetype" in dist:
                    resource["mimetype"] = dist["mediaType"]

            output.append(resource)

        return output

    def simple_transform(self, metadata: dict) -> dict:
        output = {
            # "name": "-".join(str(metadata["title"]).lower().replace(".", "").split()),
            "name": "".join([s for s in str(metadata["title"]).lower() if s.isalnum()])[
                :99
            ],  # TODO: need to add the random character suffix thing
            "owner_org": self.harvest_source.owner_org,
            "identifier": metadata["identifier"],
            "author": None,  # TODO: CHANGE THIS!
            "author_email": None,  # TODO: CHANGE THIS!
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

    def ckanify_dcatus(self) -> None:
        logger.info("ckanifying dcatus record")
        self.ckanified_metadata = self.simple_transform(self.metadata)

        self.ckanified_metadata["resources"] = self.create_ckan_resources(self.metadata)
        self.ckanified_metadata["tags"] = (
            self.create_ckan_tags(self.metadata["keyword"])
            if "keyword" in self.metadata
            else []
        )
        self.ckanified_metadata["extras"] = self.create_ckan_extras()
        logger.info("completed ckanifying dcatus record")

    def validate(self) -> None:
        logger.info(f"validating {self.identifier}")
        validator = Draft202012Validator(self.harvest_source.dataset_schema)
        try:
            validator.validate(self.metadata)
            self.valid = True
        except Exception as e:
            self.validation_msg = str(e)  # TODO: verify this is what we want
            self.valid = False

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
        ckan.action.package_create(**self.ckanified_metadata)
        self.status = "created"

    def update_record(self) -> dict:
        ckan.action.package_update(**self.ckanified_metadata)
        self.status = "updated"

    def delete_record(self) -> None:
        # purge returns nothing
        ckan.action.dataset_purge(**{"id": self.identifier})
        self.status = "deleted"

    def sync(self) -> None:
        if self.valid is False:
            logger.warning(f"{self.identifier} is invalid. bypassing {self.operation}")
            return

        self.ckanify_dcatus()

        start = datetime.now()

        if self.operation == "create":
            self.create_record()
        if self.operation == "update":
            self.update_record()

        logger.info(
            f"time to {self.operation} {self.identifier} {datetime.now()-start}"
        )

    def hydrate_from_s3(self, s3_path):
        # TODO: this method
        # set props/fields based on data
        return

    def prepare_for_s3_upload(self) -> dict:
        # TODO: confirm the minimum we want to store

        if isinstance(self.harvest_source, HarvestSource):
            self.harvest_source = self.harvest_source.title

        return json.dumps(asdict(self))

    def upload_to_s3(self, s3handler: S3Handler) -> None:
        # ruff: noqa: E501
        try:
            out_path = f"{s3handler.endpoint_url}/{self.harvest_source.title}/job-id/{self.status}/{self.identifier}.json"
            s3handler.put_object(self.prepare_for_s3_upload(), out_path)
            logger.info(
                f"saved harvest source record {self.identifier} in s3 at {out_path}"
            )
            return out_path
        except Exception as e:
            logger.error(
                f"error uploading harvest record ({self.identifier} of {self.harvest_source.title}) to s3 \n"
            )
            logger.error(
                "\n".join(traceback.format_exception(None, e, e.__traceback__))
            )

        return False
