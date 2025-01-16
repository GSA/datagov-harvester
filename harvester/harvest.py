import functools
import json
import logging
import os
import smtplib
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from itertools import groupby
from pathlib import Path
from typing import List

import requests
from boltons.setutils import IndexedSet
from ckanapi import RemoteCKAN
from jsonschema import Draft202012Validator

from harvester import SMTP_CONFIG, HarvesterDBInterface, db_interface
from harvester.exceptions import (
    CompareException,
    DCATUSToCKANException,
    ExtractExternalException,
    ExtractInternalException,
    SynchronizeException,
    TransformationException,
    ValidationException,
)
from harvester.utils.general_utils import (
    dataset_to_hash,
    download_file,
    download_waf,
    open_json,
    prepare_transform_msg,
    sort_dataset,
    traverse_waf,
)

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

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
            "schema_type",
            "source_type",
            "id",  # db guuid
            "notification_emails",
        ],
        repr=False,
    )

    _dataset_schema: dict = field(default_factory=lambda: {}, repr=False)
    _no_harvest_resp: bool = False

    # not read-only because these values are added after initialization
    # making them a "property" would require a setter which, because they are dicts,
    # means creating a custom dict class which overloads the __set_item__ method
    # worth it? not sure...
    # since python 3.7 dicts are insertion ordered so deletions will occur first
    compare_data: dict = field(
        default_factory=lambda: {
            "delete": set(),
            "create": set(),
            "update": set(),
            None: set(),
        },
        repr=False,
    )
    external_records: dict = field(default_factory=lambda: {}, repr=False)
    internal_records: dict = field(default_factory=lambda: {}, repr=False)

    _report: dict = field(default_factory=lambda: {})

    def __post_init__(self) -> None:
        self._db_interface: HarvesterDBInterface = db_interface
        self.get_source_info_from_job_id(self.job_id)

        if self.schema_type == "dcatus1.1: federal":
            self.dataset_schema = open_json(
                ROOT_DIR / "schemas" / "federal_dataset.json"
            )
        else:
            self.dataset_schema = open_json(
                ROOT_DIR / "schemas" / "non-federal_dataset.json"
            )
        self._validator = Draft202012Validator(self.dataset_schema)

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def db_interface(self) -> HarvesterDBInterface:
        return self._db_interface

    @property
    def validator(self):
        return self._validator

    @property
    def records(self):
        return self._records

    @property
    def report(self):
        return self._report

    @property
    def source_attrs(self) -> List:
        return self._source_attrs

    @property
    def dataset_schema(self) -> dict:
        return self._dataset_schema

    @dataset_schema.setter
    def dataset_schema(self, value) -> None:
        if not isinstance(value, dict):
            raise ValueError("dataset schema must be a dict")
        self._dataset_schema = value

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

    def internal_records_to_id_hash(self, records: List[dict]) -> None:
        for record in records:
            self.internal_records[record["identifier"]] = Record(
                self,
                record["identifier"],
                record["source_raw"],
                record["source_hash"],
                _ckan_id=record["ckan_id"],
                _ckan_name=record["ckan_name"],
            )

    def get_record_identifier(self, record: dict) -> str:
        record_id = "identifier" if self.schema_type.startswith("dcatus") else "url"

        if record_id not in record:
            raise Exception

        record_id = record[record_id].strip()

        if record_id == "":
            raise Exception

        return record_id

    def external_records_to_id_hash(self, records: List[dict]) -> None:
        # ruff: noqa: F841

        logger.info("converting harvest records to id: hash")
        for record in records:
            try:
                identifier = self.get_record_identifier(record)

                if self.source_type == "document":
                    dataset_hash = dataset_to_hash(sort_dataset(record))

                if self.source_type == "waf":
                    dataset_hash = dataset_to_hash(record["content"].decode("utf-8"))

                self.external_records[identifier] = Record(
                    self, identifier, record, dataset_hash
                )
            except Exception as e:
                # TODO: do something with 'e'
                raise ExtractExternalException(
                    f"{self.name} {self.url} failed to convert to id:hash",
                    self.job_id,
                )

    def prepare_external_data(self) -> None:
        logger.info("retrieving and preparing external records.")
        try:
            job = self.db_interface.get_harvest_job(self.job_id)
            if self.source_type == "document":
                if job.job_type == "clear":
                    self.external_records_to_id_hash([])
                else:
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

    def compare(self) -> None:
        """Compares records"""
        # ruff: noqa: F841
        logger.info("comparing our records with theirs")

        try:
            external_ids = IndexedSet(self.external_records.keys())
            internal_ids = IndexedSet(self.internal_records.keys())
            same_ids = external_ids & internal_ids

            self.compare_data["delete"] = internal_ids - external_ids
            self.compare_data["create"] = external_ids - internal_ids

            for i in same_ids:
                external_hash = self.external_records[i].metadata_hash
                internal_hash = self.internal_records[i].metadata_hash
                if external_hash != internal_hash:
                    self.compare_data["update"].add(i)
                else:
                    self.compare_data[None].add(i)
        except Exception as e:
            # TODO: do something with 'e'
            raise CompareException(
                f"{self.name} {self.url} failed to run compare. exiting.",
                self.job_id,
            )

    def write_compare_to_db(self) -> dict:
        records = []
        for action, ids in self.compare_data.items():
            for record_id in ids:
                if action == "delete":
                    record = self.internal_records[record_id]
                elif action == "update":
                    record = self.external_records[record_id]
                    record.ckan_id = self.internal_records[record_id].ckan_id
                    record.ckan_name = self.internal_records[record_id].ckan_name
                else:
                    record = self.external_records[record_id]

                if self.schema_type.startswith("dcatus"):
                    source_raw = json.dumps(record.metadata)
                else:
                    source_raw = record.metadata["content"]

                # set record action
                record.action = action
                db_record = {
                    "identifier": record.identifier,
                    "harvest_job_id": record.harvest_source.job_id,
                    "harvest_source_id": record.harvest_source.id,
                    "source_hash": record.metadata_hash,
                    "source_raw": source_raw,
                    "action": action,
                    "ckan_id": record.ckan_id,
                    "ckan_name": record.ckan_name,
                }
                if action is not None:
                    db_record = self.db_interface.add_harvest_record(db_record)
                    record.id = db_record.id
                records.append(record)

        # set records on new
        self._records = records

    def extract_cleanup(self):
        self.compare_data = {}
        self.internal_records = {}
        self.external_records = {}

    def extract(self) -> None:
        """determine which records needs to be updated, deleted, or created"""
        logger.info(f"getting records changes for {self.name} using {self.url}")

        self.prepare_external_data()
        self.prepare_internal_data()
        self.compare()
        self.write_compare_to_db()
        self.extract_cleanup()

    def transform(self) -> None:
        if not self.schema_type.startswith("dcatus"):
            logger.info("transforming records")
            for record in self.records:
                try:
                    record.transform()
                except TransformationException as e:
                    pass

    def validate(self) -> None:
        logger.info("validating records")
        for record in self.records:
            try:
                record.validate()
            except ValidationException as e:
                pass

    def load(self) -> None:
        """runs the delete, update, and create
        - self.compare can be empty because there was no harvest source response
        or there's truly nothing to process
        """
        logger.info("synchronizing records")
        for record in self.records:
            try:
                record.sync()

            except (
                DCATUSToCKANException,
                SynchronizeException,
                TransformationException,
            ) as e:
                pass

    def do_report(self) -> None:
        logger.info("report results")
        results = {
            "action": {"create": 0, "update": 0, "delete": 0, None: 0},
            "status": {"success": 0, "error": 0, None: 0},
            "validity": {True: 0, False: 0},
        }
        for key, group in groupby(
            self.records, lambda x: x.action if x.status != "error" else False
        ):
            results["action"][key] = sum(1 for _ in group)

        for key, group in groupby(self.records, lambda x: x.status):
            results["status"][key] = sum(1 for _ in group)

        for key, group in groupby(self.records, lambda x: x.valid):
            results["validity"][key] = sum(1 for _ in group)

        logger.info("actions completed")
        logger.info(results["action"])

        logger.info("status completed")
        logger.info(results["status"])

        logger.info("validity of records")
        logger.info(results["validity"])

        job_status = {
            "status": "complete",
            "date_finished": datetime.now(timezone.utc),
            "records_added": results["action"]["create"],
            "records_updated": results["action"]["update"],
            "records_deleted": results["action"]["delete"],
            "records_ignored": results["action"][None],
            "records_errored": results["status"]["error"],
        }
        # account for follow-up job's previous status in report count
        if "previous_job_results" in self.report:
            for key in self.report["previous_job_results"].keys():
                job_status[key] = max(
                    job_status[key] - self.report["previous_job_results"][key], 0
                )  # don't allow for negative values based on previous results
        self.db_interface.update_harvest_job(self.job_id, job_status)
        self._report = job_status

        if hasattr(self, "notification_emails") and self.notification_emails:
            self.send_notification_emails(results)

    def send_notification_emails(self, results: dict) -> None:
        try:
            job_url = f'{SMTP_CONFIG["base_url"]}/harvest_job/{self.job_id}'

            subject = "Harvest Job Completed"
            body = (
                f"The harvest job ({self.job_id}) has been successfully completed.\n"
                f"You can view the details here: {job_url}\n\n"
                "Summary of the job:\n"
                f"- Records Added: {results['action']['create']}\n"
                f"- Records Updated: {results['action']['update']}\n"
                f"- Records Deleted: {results['action']['delete']}\n"
                f"- Records Ignored: {results['action'][None]}\n"
                f"- Records Errored: {results['status']['error']}\n\n"
                "====\n"
                "You received this email because you subscribed to harvester updates.\n"
                "Please do not reply to this email, as it is not monitored."
            )
            support_recipient = SMTP_CONFIG.get("recipient")
            user_recipients = self.notification_emails
            all_recipients = [support_recipient] + user_recipients

            with smtplib.SMTP(SMTP_CONFIG["server"], SMTP_CONFIG["port"]) as server:
                if SMTP_CONFIG["use_tls"]:
                    server.starttls()
                server.login(SMTP_CONFIG["username"], SMTP_CONFIG["password"])

                for recipient in all_recipients:
                    msg = MIMEMultipart()
                    msg["From"] = SMTP_CONFIG["default_sender"]
                    msg["To"] = recipient
                    msg["Reply-To"] = "no-reply@gsa.gov"
                    msg["Subject"] = subject
                    msg.attach(MIMEText(body, "plain"))

                    server.sendmail(
                        SMTP_CONFIG["default_sender"], [recipient], msg.as_string()
                    )
                    logger.info(f"Notification email sent to: {recipient}")

        except Exception as e:
            logger.error(f"Error preparing or sending notification emails: {e}")

    def restart_job_helper(self):
        logger.info(f"restarting failed job for {self.name}")
        job = self.db_interface.get_harvest_job(self.job_id)
        updated_job = self.db_interface.update_harvest_job(
            job.id, {"status": "in_progress"}
        )
        logger.info(f"Updated job {updated_job.id} to in_progress")
        db_records = []
        for db_record in job.records:
            new_record = Record(
                self,
                db_record.identifier,
                json.loads(db_record.source_raw),
                db_record.source_hash,
                db_record.action,
                _status=db_record.status,
                _ckan_id=db_record.ckan_id,
                _ckan_name=db_record.ckan_name,
                _id=db_record.id,
            )
            db_records.append(new_record)
        self._records = db_records

    def follow_up_job_helper(self):
        logger.info(f"kicking off pickup job for {self.name}")
        db_records = self.db_interface.get_all_latest_harvest_records_by_source(self.id)
        jobs = self.db_interface.get_harvest_jobs_by_source_id(self.id)
        previous_job = jobs[-2]  # get second to last job in list
        results = {
            "previous_job_results": {
                "records_added": previous_job.records_added,
                "records_updated": previous_job.records_updated,
                "records_deleted": previous_job.records_deleted,
                "records_ignored": previous_job.records_ignored,
                "records_errored": previous_job.records_errored,
            }
        }
        self._report = results
        job = self.db_interface.get_harvest_job(self.job_id)
        updated_job = self.db_interface.update_harvest_job(
            job.id, {"status": "in_progress"}
        )
        logger.info(f"Updated job {updated_job.id} to in_progress")
        new_records = []
        for db_record in db_records:
            new_record = Record(
                self,
                db_record["identifier"],
                json.loads(db_record["source_raw"]),
                db_record["source_hash"],
                db_record["action"],
                _status=db_record["status"],
                _ckan_id=db_record["ckan_id"],
                _ckan_name=db_record["ckan_name"],
                _id=db_record["id"],
            )
            new_records.append(new_record)
        self._records = new_records


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
    _ckan_name: str = None
    _mdt_writer: str = "dcat_us"
    _mdt_msgs: str = ""
    _id: str = None

    transformed_data: dict = None
    ckanified_metadata: dict = field(default_factory=lambda: {})
    reader_map: dict = field(
        default_factory=lambda: {
            "iso19115_1": "iso19115_1",
            "iso19115_2": "iso19115_2_datagov",
            "csdgm": "fgdc",
        }
    )

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
    def id(self) -> str:
        return self._id

    @id.setter
    def id(self, value) -> None:
        self._id = value

    @property
    def ckan_name(self) -> str:
        return self._ckan_name

    @ckan_name.setter
    def ckan_name(self, value) -> None:
        self._ckan_name = value

    @property
    def mdt_writer(self) -> str:
        return self._mdt_writer

    @property
    def mdt_msgs(self) -> str:
        return self._mdt_msgs

    @mdt_msgs.setter
    def mdt_msgs(self, value) -> str:
        if not isinstance(value, str):
            raise ValueError("MDTranslator messages must be a string")
        self._mdt_msgs = value

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
            raise ValueError("validation_msg must be a string")
        self._validation_msg = value

    @property
    def status(self) -> None:
        return self._status

    @status.setter
    def status(self, value) -> None:
        if not isinstance(value, str):
            raise ValueError("status must be a string")
        self._status = value

    def transform(self) -> None:
        data = {
            "file": self.metadata["content"],
            "reader": self.reader_map[self.harvest_source.schema_type],
            "writer": self.mdt_writer,
        }

        mdt_url = os.getenv("MDTRANSLATOR_URL")
        resp = requests.post(mdt_url, json=data)
        data = resp.json()

        if resp.status_code == 422:
            self.mdt_msgs = prepare_transform_msg(data)
            raise TransformationException(
                f"record failed to transform: {self.mdt_msgs}",
                self.harvest_source.job_id,
                self.id,
            )

        if 200 <= resp.status_code < 300:
            logger.info(
                f"successfully transformed record: {self.identifier} db id: {self.id}"
            )
            self.transformed_data = json.loads(data["writerOutput"])

    def validate(self) -> None:
        logger.info(f"validating {self.identifier}")
        try:
            if self.action == "delete":
                return

            record = (
                self.metadata
                if self.transformed_data is None
                else self.transformed_data
            )

            self.harvest_source.validator.validate(record)
            self.valid = True
        except Exception as e:
            self.status = "error"
            self.validation_msg = str(e.message)
            self.valid = False
            raise ValidationException(
                repr(e),
                self.harvest_source.job_id,
                self.id,
            )

    def create_record(self, retry=False):
        from harvester.utils.ckan_utils import add_uuid_to_package_name

        try:
            result = ckan.action.package_create(**self.ckanified_metadata)
            self.ckan_id = result["id"]
            self.ckan_name = self.ckanified_metadata["name"]
        except Exception as e:
            if retry is False:
                self.ckanified_metadata["name"] = add_uuid_to_package_name(
                    self.ckanified_metadata["name"]
                )
                self.ckan_name = self.ckanified_metadata["name"]
                return self.create_record(retry=True)
            else:
                raise e
                # will be caught by outer SynchronizeException

    def update_record(self) -> dict:
        updated_metadata = {
            **self.ckanified_metadata,
            **{"id": self.ckan_id, "name": self.ckan_name},
        }
        ckan.action.package_update(**updated_metadata)

    def delete_record(self) -> None:
        ckan.action.dataset_purge(**{"id": self.ckan_id})

    def update_self_in_db(self) -> bool:
        self.status = "success"
        data = {"status": "success", "date_finished": datetime.now(timezone.utc)}
        if self.ckan_id is not None:
            data["ckan_id"] = self.ckan_id
        if self.ckan_name is not None:
            data["ckan_name"] = self.ckan_name

        self.harvest_source.db_interface.update_harvest_record(
            self.id,
            data,
        )

    def delete_self_in_db(self) -> bool:
        self.harvest_source.db_interface.delete_harvest_record(self.ckan_id)

    def ckanify_dcatus(self) -> None:
        from harvester.utils.ckan_utils import ckanify_dcatus

        try:
            self.ckanified_metadata = ckanify_dcatus(
                self.metadata, self.harvest_source, self.id
            )
        except Exception as e:
            self.status = "error"
            raise DCATUSToCKANException(repr(e), self.harvest_source.job_id, self.id)

    def sync(self) -> None:
        if self.valid is False:
            logger.warning(f"{self.identifier} is invalid. bypassing {self.action}")
            return

        if self.status == "success":
            logger.info(
                f"{self.identifier} has status 'success'. bypassing {self.action}"
            )
            return

        start = datetime.now(timezone.utc)
        # todo:
        try:
            if self.action == "delete":
                self.delete_record()
            if self.action == "create":
                self.ckanify_dcatus()
                self.create_record()
            if self.action == "update":
                self.ckanify_dcatus()
                self.update_record()
        except Exception as e:
            self.status = "error"
            raise SynchronizeException(
                f"failed to {self.action} for {self.identifier} :: {repr(e)}",
                self.harvest_source.job_id,
                self.id,
            )
        if self.action is not None:
            self.update_self_in_db()

        logger.info(
            f"time to {self.action} {self.identifier} \
                {datetime.now(timezone.utc)-start}"
        )


def harvest(jobId):
    logger.info(f"Harvest job starting for JobId: {jobId}")
    harvest_source = HarvestSource(jobId)

    # extract, compare, and save the results
    harvest_source.extract()

    # transform and validate the transform
    harvest_source.transform()
    harvest_source.validate()

    # sync with CKAN
    harvest_source.load()

    # generate harvest job report
    harvest_source.do_report()


if __name__ == "__main__":
    import sys

    from harvester.utils.general_utils import parse_args

    try:
        args = parse_args(sys.argv[1:])
        harvest(args.jobId)
    except SystemExit as e:
        logger.error(f"Harvest has experienced an error :: {repr(e)}")
        sys.exit(1)
