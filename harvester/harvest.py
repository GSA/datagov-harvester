# ruff: noqa: F841

import functools
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import ckanapi
import requests

from jsonschema import Draft202012Validator

from .ckan_utils import ckanify_dcatus
from .exceptions import (
    CompareException,
    DCATUSToCKANException,
    ExtractExternalException,
    ExtractInternalException,
    SynchronizeException,
    ValidationException,
)

from .utils import (
    dataset_to_hash,
    open_json,
    download_file,
    sort_dataset,
    get_title_from_fgdc,
    download_waf,
    traverse_waf,
)

from database.interface import HarvesterDBInterface

# requests data
session = requests.Session()
# TODD: make sure this timeout config doesn't change all requests!
session.request = functools.partial(session.request, timeout=15)

# ckan entrypoint
ckan = ckanapi.RemoteCKAN(
    os.getenv("CKAN_URL_STAGE"),
    apikey=os.getenv("CKAN_API_TOKEN_STAGE"),
    session=session,
)

# logging data
# logging.getLogger(name) follows a singleton pattern
logger = logging.getLogger("harvest_runner")

ROOT_DIR = Path(__file__).parents[1]


@dataclass
class HarvestSource:
    """Class for Harvest Sources"""

    _job_id: str
    _db_interface: HarvesterDBInterface

    _source_attrs: dict = field(
        default_factory=lambda: [
            "name",
            "url",
            "organization_id",
            "source_type",
            "id",  # db guuid
        ],
        repr=False,
    )

    _dataset_schema: dict = field(
        default_factory=lambda: open_json(ROOT_DIR / "schemas" / "dataset.json"),
        repr=False,
    )
    _no_harvest_resp: bool = False

    # not read-only because these values are added after initialization
    # making them a "property" would require a setter which, because they are dicts,
    # means creating a custom dict class which overloads the __set_item__ method
    # worth it? not sure...
    # since python 3.7 dicts are insertion ordered so deletions will occur first
    compare_data: dict = field(
        default_factory=lambda: {"delete": set(), "create": set(), "update": set()},
        repr=False,
    )
    external_records: dict = field(default_factory=lambda: {}, repr=False)
    internal_records: dict = field(default_factory=lambda: {}, repr=False)

    def __post_init__(self) -> None:
        self.get_source_info_from_job_id(self.job_id)

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def db_interface(self) -> HarvesterDBInterface:
        return self._db_interface

    @property
    def source_attrs(self) -> list:
        return self._source_attrs

    @property
    def dataset_schema(self) -> dict:
        return self._dataset_schema

    @property
    def no_harvest_resp(self) -> bool:
        return self._no_harvest_resp

    @no_harvest_resp.setter
    def no_harvest_resp(self, value) -> None:
        if not isinstance(value, bool):
            raise ValueError("No harvest response field must be a boolean")
        self._no_harvest_resp = value

    def get_source_info_from_job_id(self, job_id: str) -> None:
        # TODO: validate values here?
        source_data = self.db_interface.get_source_by_jobid(job_id)
        if source_data is None:
            raise ExtractInternalException(
                f"failed to extract source info from {job_id}. exiting",
                self.job_id,
            )
        for attr in self.source_attrs:
            setattr(self, attr, source_data[attr])

    def internal_records_to_id_hash(self, records: list[dict]) -> None:
        for record in records:
            # TODO: don't pass None to original metadata
            self.internal_records[record["identifier"]] = Record(
                self, record["identifier"], None, record["source_hash"]
            )

    def external_records_to_id_hash(self, records: list[dict]) -> None:
        # ruff: noqa: F841
        logger.info("converting harvest records to id: hash")
        for record in records:
            try:
                if self.source_type == "dcatus":
                    identifier = record["identifier"]
                    dataset_hash = dataset_to_hash(sort_dataset(record))
                if self.source_type == "waf":
                    identifier = get_title_from_fgdc(record["content"])
                    dataset_hash = dataset_to_hash(record["content"].decode("utf-8"))

                self.external_records[identifier] = Record(
                    self, identifier, record, dataset_hash
                )
            except Exception as e:
                # TODO: do something with 'e'
                raise ExtractExternalException(
                    f"{self.title} {self.url} failed to convert to id:hash",
                    self.job_id,
                )

    def prepare_internal_data(self) -> None:
        logger.info("retrieving and preparing internal records.")
        try:
            records = self.db_interface.get_harvest_record_by_source(self.id)
            self.internal_records_to_id_hash(records)
        except Exception as e:
            raise ExtractInternalException(
                f"{self.name} {self.url} failed to extract internal records. exiting",
                self.job_id,
            )

    def prepare_external_data(self) -> None:
        logger.info("retrieving and preparing external records.")
        try:
            if self.source_type == "dcatus":
                self.external_records_to_id_hash(
                    download_file(self.url, ".json")["dataset"]
                )
            if self.source_type == "waf":
                # TODO
                self.external_records_to_id_hash(download_waf(traverse_waf(self.url)))
        except Exception as e:
            raise ExtractExternalException(
                f"{self.name} {self.url} failed to extract harvest source. exiting",
                self.job_id,
            )

    def compare(self) -> None:
        """Compares records"""
        # ruff: noqa: F841
        logger.info("comparing our records with theirs")

        try:
            external_ids = set(self.external_records.keys())
            internal_ids = set(self.internal_records.keys())
            same_ids = external_ids & internal_ids

            self.compare_data["delete"] = internal_ids - external_ids
            self.compare_data["create"] = external_ids - internal_ids

            for i in same_ids:
                external_hash = self.external_records[i].metadata_hash
                internal_hash = self.internal_records[i].metadata_hash
                if external_hash != internal_hash:
                    self.compare_data["update"].add(i)
        except Exception as e:
            # TODO: do something with 'e'
            raise CompareException(
                f"{self.title} {self.url} failed to run compare. exiting.",
                self.job_id,
            )

    def get_record_changes(self) -> None:
        """determine which records needs to be updated, deleted, or created"""
        logger.info(f"getting records changes for {self.name} using {self.url}")
        self.prepare_internal_data()
        self.prepare_external_data()
        self.compare()

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
                        self.external_records[i] = Record(
                            self, self.internal_records[i].identifier
                        )
                        self.external_records[i].operation = operation
                        self.external_records[i].delete_record()
                        continue

                    record = self.external_records[i]
                    # no longer setting operation in compare so setting it here...
                    record.operation = operation

                    record.validate()
                    # TODO: add transformation and validation
                    record.sync()

                except (
                    ValidationException,
                    DCATUSToCKANException,
                    SynchronizeException,
                ) as e:
                    # TODO: do something with 'e'?
                    pass

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

        for record_id, record in self.external_records.items():
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

    def validate(self) -> None:
        logger.info(f"validating {self.identifier}")
        # ruff: noqa: F841
        validator = Draft202012Validator(self.harvest_source.dataset_schema)
        try:
            validator.validate(self.metadata)
            self.valid = True
        except Exception as e:
            self.validation_msg = str(e)  # TODO: verify this is what we want
            self.valid = False
            # TODO: do something with 'e' in logger?
            raise ValidationException(
                f"{self.identifier} failed validation",
                self.harvest_source.job_id,
                self.identifier,
            )

    def create_record(self):
        ckan.action.package_create(**self.ckanified_metadata)
        self.status = "created"

    def update_record(self) -> dict:
        ckan.action.package_update(**self.ckanified_metadata)
        self.status = "updated"

    def delete_record(self) -> None:
        ckan.action.dataset_purge(**{"id": self.identifier})
        self.status = "deleted"

    def ckanify_dcatus(self) -> None:
        try:
            self.ckanified_metadata = ckanify_dcatus(
                self.metadata, self.harvest_source.organization_id
            )
        except Exception as e:
            raise DCATUSToCKANException(
                f"failed to convert dcatus to ckan for {self.identifier}",
                self.harvest_source.job_id,
                self.harvest_source.name,
            )

    def sync(self) -> None:
        # ruff: noqa: F841
        if self.valid is False:
            logger.warning(f"{self.identifier} is invalid. bypassing {self.operation}")
            return

        self.ckanify_dcatus()

        start = datetime.now()

        try:
            if self.operation == "create":
                self.create_record()
            if self.operation == "update":
                self.update_record()
        except Exception as e:
            # TODO: something with 'e'
            raise SynchronizeException(
                f"failed to {self.operation} for {self.identifier}",
                self.harvest_source.job_id,
                self.identifier,
            )

        logger.info(
            f"time to {self.operation} {self.identifier} {datetime.now()-start}"
        )
