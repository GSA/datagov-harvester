# ruff: noqa: F841
# ruff: noqa: E402
import functools
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import requests
from ckanapi import RemoteCKAN
from jsonschema import Draft202012Validator

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

from harvester import HarvesterDBInterface, db_interface
from harvester.exceptions import (
    CompareException,
    DCATUSToCKANException,
    ExtractExternalException,
    ExtractInternalException,
    SynchronizeException,
    ValidationException,
)
from harvester.utils.ckan_utils import ckanify_dcatus
from harvester.utils.general_utils import (
    dataset_to_hash,
    download_file,
    download_waf,
    get_title_from_fgdc,
    open_json,
    sort_dataset,
    traverse_waf,
)

# requests data
session = requests.Session()
# TODD: make sure this timeout config doesn't change all requests!
session.request = functools.partial(session.request, timeout=15)

# ckan entrypoint
ckan = RemoteCKAN(
    os.getenv("CKAN_API_URL"),
    apikey=os.getenv("CKAN_API_TOKEN"),
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
        self._db_interface: HarvesterDBInterface = db_interface
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
        try:
            source_data = self.db_interface.get_harvest_source_by_jobid(job_id)
            for attr in self.source_attrs:
                setattr(self, attr, getattr(source_data, attr))
        except Exception as e:
            raise ExtractInternalException(
                f"failed to extract source info from {job_id}. exiting",
                self.job_id,
            )

    def internal_records_to_id_hash(self, records: list[dict]) -> None:
        for record in records:
            # TODO: don't pass None to original metadata
            self.internal_records[record["identifier"]] = Record(
                self,
                record["identifier"],
                record["source_raw"],
                record["source_hash"],
                _ckan_id=record["ckan_id"],
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
            records = self.db_interface.get_latest_harvest_records_by_source(self.id)
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

    def write_compare_to_db(self) -> dict:
        records = []

        for action, ids in self.compare_data.items():
            for record_id in ids:
                if action == "delete":
                    record = self.internal_records[record_id]
                else:
                    record = self.external_records[record_id]

                records.append(
                    {
                        "identifier": record.identifier,
                        "harvest_job_id": record.harvest_source.job_id,
                        "harvest_source_id": record.harvest_source.id,
                        "source_hash": record.metadata_hash,
                        "source_raw": str(record.metadata),
                        "action": action,
                        "ckan_id": record.ckan_id,
                    }
                )

        self.internal_records_lookup_table = self.db_interface.add_harvest_records(
            records
        )

    def synchronize_records(self) -> None:
        """runs the delete, update, and create
        - self.compare can be empty becuase there was no harvest source response
        or there's truly nothing to process
        """
        logger.info("synchronizing records")
        for action, ids in self.compare_data.items():
            for i in ids:
                try:
                    if action == "delete":
                        # we don't actually create a Record instance for deletions
                        # so creating it here as a sort of acknowledgement
                        self.external_records[i] = Record(
                            self,
                            self.internal_records[i].identifier,
                            _ckan_id=self.internal_records[i].ckan_id,
                        )
                        self.external_records[i].action = action
                        try:
                            self.external_records[i].delete_record()
                            self.external_records[i].update_self_in_db()
                        except Exception as e:
                            self.external_records[i].status = "error"
                            raise SynchronizeException(
                                f"failed to {self.external_records[i].action} \
                                    for {self.external_records[i].identifier} :: \
                                        {repr(e)}",
                                self.job_id,
                                self.internal_records_lookup_table[
                                    self.external_records[i].identifier
                                ],
                            )
                        continue

                    record = self.external_records[i]
                    if action == "update":
                        record.ckan_id = self.internal_records[i].ckan_id

                    # no longer setting action in compare so setting it here...
                    record.action = action

                    record.validate()
                    # TODO: add transformation
                    record.sync()

                except (
                    ValidationException,
                    DCATUSToCKANException,
                    SynchronizeException,
                ) as e:
                    pass

    def report(self) -> None:
        logger.info("report results")
        # log our original compare data
        logger.info("expected actions to be done")
        logger.info({action: len(ids) for action, ids in self.compare_data.items()})

        # validation count and actual results
        actual_results_action = {
            "delete": 0,
            "update": 0,
            "create": 0,
            None: 0,
        }
        actual_results_status = {"success": 0, "error": 0, None: 0}
        validity = {"valid": 0, "invalid": 0}

        for record_id, record in self.external_records.items():
            # action
            actual_results_action[record.action] += 1
            # status
            actual_results_status[record.status] += 1
            # validity
            if record.valid:
                validity["valid"] += 1
            else:
                validity["invalid"] += 1

        # what actually happened?
        logger.info("actual actions completed")
        logger.info(actual_results_action)

        # what actually happened?
        logger.info("actual status completed")
        logger.info(actual_results_action)

        # what's our record validity count?
        logger.info("validity of the records")
        logger.info(validity)

        job_status = {
            "status": "complete",
            "date_finished": datetime.now(timezone.utc),
            "records_added": actual_results_action["create"],
            "records_updated": actual_results_action["update"],
            "records_deleted": actual_results_action["delete"],
            "records_ignored": actual_results_action[None],
            "records_errored": actual_results_status["error"],
        }
        self.db_interface.update_harvest_job(self.job_id, job_status)


@dataclass
class Record:
    """Class for Harvest Records"""

    _harvest_source: HarvestSource
    _identifier: str
    _metadata: dict = field(default_factory=lambda: {})
    _metadata_hash: str = ""
    _action: str = None
    _valid: bool = None
    _validation_msg: str = ""
    _status: str = None
    _ckan_id: str = None

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
    def ckan_id(self) -> str:
        return self._ckan_id

    @ckan_id.setter
    def ckan_id(self, value) -> None:
        self._ckan_id = value

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
    def action(self) -> str:
        return self._action

    @action.setter
    def action(self, value) -> None:
        if value not in ["create", "update", "delete", None]:
            raise ValueError("Unknown action being set to record")
        self._action = value

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
            self.status = "error"
            self.validation_msg = str(e)  # TODO: verify this is what we want
            self.valid = False
            raise ValidationException(
                repr(e),
                self.harvest_source.job_id,
                self.harvest_source.internal_records_lookup_table[self.identifier],
            )

    def create_record(self):
        result = ckan.action.package_create(**self.ckanified_metadata)
        self.ckan_id = result["id"]

    def update_record(self) -> dict:
        ckan.action.package_update(**self.ckanified_metadata, **{"id": self.ckan_id})

    def delete_record(self) -> None:
        ckan.action.dataset_purge(**{"id": self.ckan_id})

    def update_self_in_db(self) -> bool:
        self.status = "success"
        data = {"status": "success", "date_finished": datetime.now(timezone.utc)}
        if self.ckan_id is not None:
            data["ckan_id"] = self.ckan_id

        self.harvest_source.db_interface.update_harvest_record(
            self.harvest_source.internal_records_lookup_table[self.identifier],
            data,
        )

    def ckanify_dcatus(self) -> None:
        try:
            self.ckanified_metadata = ckanify_dcatus(
                self.metadata, self.harvest_source.organization_id
            )
        except Exception as e:
            self.status = "error"
            raise DCATUSToCKANException(
                repr(e),
                self.harvest_source.job_id,
                self.harvest_source.internal_records_lookup_table[self.identifier],
            )

    def sync(self) -> None:
        # ruff: noqa: F841
        if self.valid is False:
            logger.warning(f"{self.identifier} is invalid. bypassing {self.action}")
            return

        self.ckanify_dcatus()

        start = datetime.now(timezone.utc)

        try:
            if self.action == "create":
                self.create_record()
            if self.action == "update":
                self.update_record()
        except Exception as e:
            self.status = "error"
            raise SynchronizeException(
                f"failed to {self.action} for {self.identifier} :: {repr(e)}",
                self.harvest_source.job_id,
                self.harvest_source.internal_records_lookup_table[self.identifier],
            )
        self.update_self_in_db()

        logger.info(
            f"time to {self.action} {self.identifier} \
                {datetime.now(timezone.utc)-start}"
        )


def harvest(jobId):
    logger.info(f"Harvest job starting for JobId: {jobId}")
    harvest_source = HarvestSource(jobId)
    harvest_source.get_record_changes()
    harvest_source.write_compare_to_db()
    harvest_source.synchronize_records()
    harvest_source.report()


if __name__ == "__main__":
    import sys

    from harvester.utils.general_utils import parse_args

    try:
        args = parse_args(sys.argv[1:])
        harvest(args.jobId)
    except SystemExit as e:
        logger.error(f"Harvest has experienced an error :: {repr(e)}")
        sys.exit(1)
