import gc
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from itertools import chain
from pathlib import Path
from typing import List

import requests
from jsonschema import Draft202012Validator, FormatChecker
from requests.exceptions import HTTPError, Timeout

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))


from database.models import HarvestSource as HarvestSourceORM

# ruff: noqa: E402
from harvester import SMTP_CONFIG, HarvesterDBInterface, db_interface
from harvester.exceptions import (
    CKANDownException,
    CKANRejectionException,
    ClearJobException,
    CompareException,
    DCATUSToCKANException,
    DuplicateIdentifierException,
    ExternalRecordToClass,
    ExtractExternalException,
    ExtractInternalException,
    SendNotificationException,
    SynchronizeException,
    TransformationException,
    log_non_critical_error,
)
from harvester.lib.cf_handler import CFHandler
from harvester.lib.harvest_reporter import HarvestReporter
from harvester.lib.load_manager import LoadManager
from harvester.utils.ckan_utils import CKANSyncTool
from harvester.utils.general_utils import (
    DT_PLACEHOLDER,
    USER_AGENT,
    assemble_validation_errors,
    create_retry_session,
    dataset_to_hash,
    download_file,
    find_indexes_for_duplicates,
    get_datetime,
    make_record_mapping,
    open_json,
    prepare_transform_msg,
    send_email_to_recipients,
    sort_dataset,
    traverse_waf,
)

# requests data
session = create_retry_session()

ckan_sync_tool = CKANSyncTool(session=session)

# logging data
logger = logging.getLogger("harvest_runner")

ROOT_DIR = Path(__file__).parents[1]

# harvest worker count
harvest_worker_sync_count = int(os.getenv("HARVEST_WORKER_SYNC_COUNT", 1))

CF_API_URL = os.getenv("CF_API_URL")
CF_SERVICE_USER = os.getenv("CF_SERVICE_USER")
CF_SERVICE_AUTH = os.getenv("CF_SERVICE_AUTH")


@dataclass
class HarvestSource:
    """Class for Harvest Sources"""

    _job_id: str
    _job_type: str = "harvest"

    _source_attrs: dict = field(
        default_factory=lambda: [
            "name",
            "url",
            "organization_id",
            "schema_type",
            "source_type",
            "collection_parent_url",
            "id",  # db guuid
            "notification_emails",
            "notification_frequency",
        ],
        repr=False,
    )
    _records: list = field(default_factory=lambda: [], repr=False)
    _dataset_schema: dict = field(default_factory=lambda: {}, repr=False)
    _no_harvest_resp: bool = False
    _clear_complete: bool = True

    external_records: dict = field(default_factory=lambda: {}, repr=False)
    internal_records: dict = field(default_factory=lambda: {}, repr=False)

    deletions: set = field(default_factory=lambda: set(), repr=False)

    def __post_init__(self) -> None:
        self._db_interface: HarvesterDBInterface = db_interface
        self.get_source_info_from_job_id(self.job_id)

        self.schemas_root = ROOT_DIR / "schemas"

        if self.schema_type == "dcatus1.1: federal":
            self.schema_file = self.schemas_root / "federal_dataset.json"
        elif self.schema_type in ["dcatus1.1: non-federal"]:
            self.schema_file = self.schemas_root / "non-federal_dataset.json"
        elif self.schema_type.startswith("iso19115"):
            self.schema_file = self.schemas_root / "iso-non-federal_dataset.json"
        else:
            # this can't happen because we apply an enum in our model but just in case.
            logger.error(
                f"unacceptable schema type: {self.schema_type}. exiting harvest."
            )
            self.finish_job_with_status("error")
            raise Exception

        self.dataset_schema = open_json(self.schema_file)
        self._validator = Draft202012Validator(
            self.dataset_schema, format_checker=FormatChecker()
        )
        self._reporter = HarvestReporter()

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def job_type(self) -> str:
        return self._job_type

    @property
    def db_interface(self) -> HarvesterDBInterface:
        return self._db_interface

    @property
    def validator(self):
        return self._validator

    @property
    def records(self):
        return self._records

    @records.setter
    def records(self, value) -> None:
        self._records = value

    @property
    def reporter(self):
        return self._reporter

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

    @property
    def clear_complete(self) -> bool:
        return self._clear_complete

    @clear_complete.setter
    def clear_complete(self, value) -> None:
        if not isinstance(value, bool):
            raise ValueError("Clear complete must be a boolean")
        self._clear_complete = value

    def get_source_info_from_job_id(self, job_id: str) -> None:
        # TODO: validate values here?
        try:
            source_data = self.db_interface.get_harvest_source_by_jobid(job_id)
            for attr in self.source_attrs:
                setattr(self, attr, getattr(source_data, attr))
        except Exception as e:
            raise ExtractInternalException(
                f"failed to extract source info from {job_id}. exiting :: {repr(e)}",
                self.job_id,
            )

    def update_job_record_count_by_action(self, action: str):
        """updates the record action counter in the reporter
        and updates the job with new count dict

        record actions are:
            create
            update
            delete
            None (unchanged)
            errored
            validated
        """
        self.reporter.update(action)
        self.db_interface.update_harvest_job(self.job_id, self.reporter.report())

    def get_source_orm(self) -> HarvestSourceORM:
        """Get the harvest source object from the database."""
        return self.db_interface.get_harvest_source_by_jobid(self.job_id)

    def store_records_as_internal(self, records: List[dict]) -> None:
        """
        converts the list of db records into Record instances and stores them
        in self.internal_records

        records: list of internal db harvest record dicts
        """
        for record in records:
            self.internal_records[record["identifier"]] = Record(
                self,
                record["identifier"],
                None,  # source raw
                record["source_hash"],
                _ckan_id=record["ckan_id"],
                _ckan_name=record["ckan_name"],
                _date_finished=record["date_finished"],
                _parent_identifier=record.get("parent_identifier"),
            )

    def write_duplicate_to_db(self, identifier: str) -> None:
        """
        duplicates are identified by index prior to function call so every input to this
        function is a duplicate. given the duplicate identifier write to the db as a
        duplicate identifier record error.

        identifier: identifier of the record ("identifier" in datajson or full xml path in waf)
        """
        # Create a minimal harvest_record for error tracking purposes only
        record_data = {
            "harvest_job_id": self.job_id,
            "harvest_source_id": self.id,
            "identifier": identifier,
            "status": "error",
        }

        # Insert the record so it can be referenced in the error table
        new_record = self.db_interface.add_harvest_record(record_data)
        harvest_record_id = new_record.id if new_record else None

        raise DuplicateIdentifierException(
            f"Duplicate identifier '{identifier}' found for source: {self.name}",
            self.job_id,
            harvest_record_id,
        )

    def filter_duplicate_identifiers(self) -> None:
        """
        this function identifies duplicates in the harvest source via the record "identifier".
        it adds a record error about the duplicate to the db and removes the duplicate from processing.
        """
        indices = find_indexes_for_duplicates(self.external_records)
        for idx in indices:
            try:
                self.write_duplicate_to_db(self.external_records[idx]["identifier"])
            except DuplicateIdentifierException:
                del self.external_records[idx]
                self.update_job_record_count_by_action("errored")

    def filter_waf_files_by_datetime(self) -> None:
        """
        retains waf files which need to be created or updated whereby the update
        is determined via the modified_date found on the waf web page. if the file date
        at the source is more recent than what we have then we want to add it for processing.

        this function is only called when the harvest source type is "waf" so
        self.external_records would be a list of dictionaries like...
        [ { "identifier": "a.xml", "modified_date": datetime_obj}, ... ]
        """
        records = []
        for data in self.external_records:
            identifier = data["identifier"]
            internal_record = self.internal_records.get(identifier, None)
            if internal_record is not None:
                # date_finished should never be None since we
                # only grab successful internal records which are datetime stamped
                if data["modified_date"] > internal_record.date_finished:
                    records.append(data)  # update
            else:
                records.append(data)  # create
        self.external_records = records

    def determine_internal_deletions(self) -> None:
        """
        determines which records in the harvester db needed to be deleted based on
        the identifiers of the records. if the db has the record and the harvest source
        doesn't that means the record needs to be deleted.
        """
        external_ids = set([record["identifier"] for record in self.external_records])
        internal_ids = set(self.internal_records.keys())
        self.deletions = internal_ids - external_ids

    def iter_internal_records_to_be_deleted(self) -> any:
        """
        given a list of records to delete grab the internal Record instance, update
        the instance action to "delete", write that data to the db and yield it for
        processing.
        """
        for identifier in self.deletions:
            internal_record = self.internal_records[identifier]
            internal_record.action = "delete"
            internal_record.write_compare_to_db()
            yield internal_record

    def external_records_to_process(self) -> any:
        """
        this function prepares the external record for ETVL processing by converting
        each record into a Record instance by providing the associated harvest source,
        the record identifier, and record itself, and the hash of the record.

        at the start of the loop the record is either an xml url string (waf) or a
        json/dict object (datajson). in the case for waf this function downloads
        the file. in the case for datajson the dict is recursively sorted
        to ensure accurate hashes across harvests.

        this function yields 1 record at a time to minimize memory usage of large waf sources.
        it yields 1 record regardless of the source type (e.g. datajson or waf)
        """
        while len(self.external_records) > 0:
            try:
                record = self.external_records.pop(0)
                parent_identifier = None
                if self.source_type == "waf":
                    record["content"] = download_file(record["identifier"], ".xml")
                    dataset = record["content"]

                elif self.source_type == "waf-collection":
                    record["content"] = download_file(record["identifier"], ".xml")
                    dataset = record["content"]
                    parent_identifier = self.collection_parent_url

                elif self.source_type == "document":
                    if self.schema_type.startswith("dcatus"):
                        dataset = json.dumps(sort_dataset(record))
                    elif self.schema_type.startswith("iso19115"):
                        # single document ISO
                        record["content"] = download_file(record["identifier"], ".xml")
                        dataset = record["content"]

                dataset_hash = dataset_to_hash(dataset)

                yield Record(
                    self,
                    record["identifier"],
                    dataset,
                    dataset_hash,
                    _parent_identifier=parent_identifier,
                )

                del record
            except Exception as e:
                self.update_job_record_count_by_action("errored")

                # "record"s in self.external_records are standardized as dicts at this point
                record_id = record["identifier"] if "identifier" in record else None

                ExternalRecordToClass(
                    f"{self.name} {record_id} failed to prepare record for harvest :: {repr(e)}",
                    self.job_id,
                    None,  # there is no record id to associate
                )

    def acquire_minimum_external_data(self) -> list:
        """
        this function either downloads the datajson file or gathers all the waf files
        to be downloaded later. self.external_records is a list of dicts. for waf,
        the dict has a similar "schema" to dcatus for standardization across this app.

        "minimum" means the least we need to move forward with
        harvesting. if the minimum can't be met then we throw a critical exception.
        """
        logger.info("retrieving external records.")
        try:
            if self.source_type == "document":
                if self.schema_type.startswith("dcatus"):
                    self.external_records = download_file(self.url, ".json")["dataset"]
                elif self.schema_type.startswith("iso19115"):
                    # mimic the output of traverse_waf with a single file
                    self.external_records = [{"identifier": self.url}]
                else:
                    raise ValueError(f"Schema type {self.schema_type} is not supported")

            elif self.source_type == "waf":
                self.external_records = traverse_waf(self.url)

            elif self.source_type == "waf-collection":
                # First element is just like a single document waf, but we
                # don't have a datetime for it, so use the ancient placeholder
                self.external_records = [{"identifier": self.collection_parent_url,
                                          "modified_date": DT_PLACEHOLDER}]
                self.external_records += traverse_waf(self.url)

        except Exception as e:
            # ruff: noqa: E501
            raise ExtractExternalException(
                f"{self.name} {self.url} failed to extract harvest source. exiting :: {repr(e)}",
                self.job_id,
            )

    def acquire_data_sources(self) -> None:
        """
        retrieves external (harvest source) and internal (harvester db) data sources
        """
        self.acquire_minimum_internal_data()

        if self.job_type == "clear":
            self.external_records = []
        else:
            self.acquire_minimum_external_data()

    def acquire_minimum_internal_data(self) -> None:
        """
        this function retrieves the latest set of records for the given harvest source
        and converts them into Record instances. these records don't contain the original
        raw source because the app doesn't need it and by excluding it the app uses
        less memory.
        """
        logger.info("retrieving and preparing internal records.")
        try:
            records = self.db_interface.get_latest_harvest_records_by_source(self.id)
            self.store_records_as_internal(records)
        except Exception as e:
            # ruff: noqa: E501
            raise ExtractInternalException(
                f"{self.name} {self.url} failed to extract internal records. exiting :: {repr(e)}",
                self.job_id,
            )

    def run_full_harvest(self) -> None:
        try:
            self.acquire_data_sources()

            self.determine_internal_deletions()
            internal_records_to_delete = self.iter_internal_records_to_be_deleted()

            if self.source_type in ["waf", "waf-collection"]:
                self.filter_waf_files_by_datetime()

            self.filter_duplicate_identifiers()

            external_records_to_process = self.external_records_to_process()

            # amount of work to be done
            self.reporter.total = len(self.deletions) + len(self.external_records)

            # deletions would occur first based on the arg positions
            records = chain(internal_records_to_delete, external_records_to_process)

            for record in records:
                record.harvest()
                del record
                gc.collect()

        except (ExtractInternalException, ExtractExternalException):
            self.finish_job_with_status("error")
            return

    def finish_job_with_status(self, status: str):
        """
        update the job record in the db with the provided status and set
        date_finished. date_finished is set here because this function is
        either called on critical exception or the job has completed harvesting.
        """
        self.db_interface.update_harvest_job(
            self.job_id, {"status": status, "date_finished": get_datetime()}
        )

    def report(self) -> None:
        """Assemble and record report for harvest job"""

        job_results = self.reporter.report()

        if self.job_type == "clear" and self.clear_complete is False:
            ClearJobException(
                f"{self.name} failed to clear completely",
                self.job_id,
            )

        # only label the job as "complete" if it hasn't errored out by now
        job = self.db_interface.get_harvest_job(self.job_id)
        if job.status != "error":
            job_status = {"status": "complete", "date_finished": get_datetime()}
            job_status.update(job_results)
            self.db_interface.update_harvest_job(self.job_id, job_status)

        if hasattr(self, "notification_emails") and self.notification_emails:
            if self.notification_frequency == "always" or (
                self.notification_frequency == "on_error"
                and job_results["records_errored"]
            ):
                try:
                    self.send_notification_emails(job_results)
                except SendNotificationException as e:
                    logging.error(
                        f"Error sending notification emails for job {self.job_id}: {e}"
                    )

    def send_notification_emails(self, job_results: dict) -> None:
        """Send harvest report emails to havest source POCs"""
        try:
            job_url = f"{SMTP_CONFIG['base_url']}/harvest_job/{self.job_id}"

            subject = "Harvest Job Completed"
            body = (
                f"The harvest job ({self.job_id}) has been successfully completed.\n"
                f"You can view the details here: {job_url}\n\n"
                "Summary of the job:\n"
                f"- Records Added: {job_results['records_added']}\n"
                f"- Records Updated: {job_results['records_updated']}\n"
                f"- Records Deleted: {job_results['records_deleted']}\n"
                f"- Records Unchanged: {job_results['records_ignored']}\n"
                f"- Records Errored: {job_results['records_errored']}\n"
                f"- Records Validated: {job_results['records_validated']}\n\n"
                "====\n"
                "You received this email because you subscribed to harvester updates.\n"
                "Please do not reply to this email, as it is not monitored."
            )
            support_recipient = SMTP_CONFIG.get("recipient")
            user_recipients = self.notification_emails
            all_recipients = [support_recipient] + user_recipients

            send_email_to_recipients(all_recipients, subject, body)

        except Exception as e:
            logger.error(f"Error preparing or sending notification emails: {e}")
            raise SendNotificationException(
                f"Error preparing or sending notification emails for job {self.job_id}: {e}",
                self.job_id,
            )


@dataclass
class Record:
    """Class for Harvest Records"""

    _harvest_source: HarvestSource
    _identifier: str
    _source_raw: str = None
    _metadata_hash: str = ""
    _action: str = None
    _status: str = None
    _ckan_id: str = None
    _ckan_name: str = None
    _date_finished: str = None
    _mdt_writer: str = "dcat_us"
    _mdt_msgs: str = ""
    _id: str = None
    _parent_identifier: str = None

    transformed_data: dict = None
    ckanified_metadata: dict = field(default_factory=lambda: {})
    reader_map: dict = field(
        default_factory=lambda: {
            "iso19115_1": "iso19115_2_datagov",
            "iso19115_2": "iso19115_2_datagov",
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
    def date_finished(self) -> str:
        return self._date_finished

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
    def parent_identifier(self) -> str:
        return self._parent_identifier

    @property
    def source_raw(self) -> str:
        return self._source_raw

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
    def status(self) -> None:
        return self._status

    @status.setter
    def status(self, value) -> None:
        if not isinstance(value, str):
            raise ValueError("status must be a string")
        self._status = value

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        """Return whether a string is a valid URL."""
        return Draft202012Validator(
            {"type": "string", "format": "uri"},
            format_checker=Draft202012Validator.FORMAT_CHECKER,
        ).is_valid(url)

    def is_valid_describedByType(self, described_by_type: str) -> bool:
        """Return whether a string is a valid describedByType."""

        return Draft202012Validator(
            self.harvest_source.dataset_schema["properties"]["describedByType"],
            format_checker=FormatChecker(),
        ).is_valid(described_by_type)

    def harvest(self) -> None:
        """
        this is the main harvest function for a record instance. it runs the compare,
        transform (when needed), DCATUS v1.1 validation, and synchronization with ckan. this process
        handles the create, update, delete, and do-nothing (just skips).
        """
        try:
            if self.action == "delete":
                synched_with_ckan = self.sync()
                if (
                    self.harvest_source.job_type == "clear"
                    and synched_with_ckan is False
                ):
                    self.harvest_source.clear_complete = False
                return
            self.compare()
            if self.action is None:
                return
            if self.harvest_source.schema_type.startswith("iso19115"):
                self.transform()
                self.add_parent()
                self.fill_placeholders()
            self.validate()
            self.sync()
        except (
            CompareException,
            TransformationException,
        ):
            pass

    def compare(self) -> None:
        """
        this function determines the work (or lack of work) needing to be
        done on the record instance. the work is either create, update, or
        None. None means the record is synchronized with the source and nothing
        needs to be done. we have the latest state of the record.
        """
        internal_record = self.harvest_source.internal_records.get(
            self.identifier, None
        )

        if internal_record is not None:
            not_same_hash = internal_record.metadata_hash != self.metadata_hash
            # TODO: should a force-harvest be an update or a create?
            if not_same_hash or self.harvest_source.job_type == "force_harvest":
                self.action = "update"
                self.ckan_id = internal_record.ckan_id
                self.ckan_name = internal_record.ckan_name
        else:
            self.action = "create"

        if self.action is not None:
            self.write_compare_to_db()
        else:
            self.harvest_source.update_job_record_count_by_action(None)

    def write_compare_to_db(self) -> None:
        try:
            record_mapping = make_record_mapping(self)
            db_record = self.harvest_source.db_interface.add_harvest_record(
                record_mapping
            )
            self.id = db_record.id
        except Exception as e:
            raise CompareException(
                f"{self.harvest_source.name} {self.harvest_source.url} failed to write compare to db. :: {repr(e)}",
                self.harvest_source.job_id,
            )

    def transform(self) -> None:
        data = {
            "file": self.source_raw,
            "reader": self.reader_map[self.harvest_source.schema_type],
            "writer": self.mdt_writer,
        }

        mdt_url = os.getenv("MDTRANSLATOR_URL")
        try:
            resp = requests.post(mdt_url, json=data, headers={"User-Agent": USER_AGENT})
            # this will raise an HTTPError for bad responses (4xx, 5xx)
            # so we can handle them in the except block, we also will have
            # access to the response object
            resp.raise_for_status()
            if 200 <= resp.status_code < 300:
                data = resp.json()
                logger.info(
                    f"successfully transformed record: {self.identifier} db id: {self.id}"
                )
                self.transformed_data = json.loads(data["writerOutput"])

        except HTTPError as err:
            logger.error("Error: %s - Status Code: %s", err, resp.status_code)
            if resp.status_code == 422:
                data = resp.json()
                self.mdt_msgs = prepare_transform_msg(data)
                self.status = "error"
                self.harvest_source.update_job_record_count_by_action("errored")
                raise TransformationException(
                    f"record failed to transform: {self.mdt_msgs}",
                    self.harvest_source.job_id,
                    self.id,
                )
            else:
                self.status = "error"
                self.harvest_source.update_job_record_count_by_action("errored")
                raise TransformationException(
                    f"record failed to transform because of unexpected status code: {resp.status_code}",
                    self.harvest_source.job_id,
                    self.id,
                )

        except Timeout:
            logger.error("Request timed out")
            self.status = "error"
            self.harvest_source.update_job_record_count_by_action("errored")
            raise TransformationException(
                "record failed to transform due to request timeout",
                self.harvest_source.job_id,
                self.id,
            )

        except Exception as err:
            logger.info("Unexpected error: %s", err)
            self.status = "error"
            self.harvest_source.update_job_record_count_by_action("errored")
            raise TransformationException(
                f"record failed to transform with error: {err}",
                self.harvest_source.job_id,
                self.id,
            )

    def add_parent(self) -> None:
        """Add parent information to transformed_data for waf-collections."""
        if self.parent_identifier is None:
            return

        if "isPartOf" in self.transformed_data:
            # somehow this record already got a parent relationship!
            return

        self.transformed_data["isPartOf"] = self.parent_identifier

    def fill_placeholders(self) -> None:
        """Fill in placeholder values to prevent some validation errors.

        We work directly on the self.transformed_data dict.
        """
        # missing contactPoint or it's empty
        if not self.transformed_data.get("contactPoint"):
            self.transformed_data["contactPoint"] = {
                "fn": "Not provided - Contact data.gov",
                "hasEmail": "mailto:datagovsupport@gsa.gov",
            }

        if not self.transformed_data.get("description"):
            self.transformed_data["description"] = "No description was provided."

        if not self.transformed_data.get("keyword"):
            self.transformed_data["keyword"] = ["__"]

        if not self.transformed_data.get("publisher"):
            # publisher defaults to the harvest source's organization
            # information
            self.transformed_data["publisher"] = {
                "name": self.harvest_source.get_source_orm().org.name
            }

        if not self.is_valid_describedByType(
            self.transformed_data.get("describedByType", "")
        ):
            self.transformed_data["describedByType"] = "application/octet-steam"

        # If distribution items have a downloadURL or accessURL,
        # check if it just needs an "https://" at the beginning
        # to be valid
        def _guess_better_url_in_item(item, key):
            url = item.get(key)
            if url is not None and not self._is_valid_url(url):
                # it exists and isn't valid
                candidate = "https://" + url
                if self._is_valid_url(candidate):
                    # TODO: log a warning that we are making this change
                    item[key] = candidate

        for dist_item in self.transformed_data.get("distribution", []):
            _guess_better_url_in_item(dist_item, "downloadURL")
            _guess_better_url_in_item(dist_item, "accessURL")
            if not self.is_valid_describedByType(dist_item.get("describedByType", "")):
                dist_item["describedByType"] = "application/octet-steam"

    def _report_error(self, e):
        """Report an exception to the database.

        This does not re-raise the exception, it logs it and
        execution proceeds. If callers want to raise, they need
        to `raise e` after calling this method.
        """
        self.status = "error"
        log_non_critical_error(
            repr(e),
            self.harvest_source.job_id,
            self.id,
            e.__class__.__name__,
            emit_log=False,
        )

    def validate(self) -> None:
        """Validate a single record.

        Run our jsconschema validator against the raw JSON record or the
        transformed JSON data.

        Returns True if the record is valid, False if it is not.
        """
        # TODO: create a different status for transformation exceptions
        # so they aren't confused with validation issues
        logger.info(f"validating {self.identifier}")
        if self.transformed_data is None:
            try:
                record = json.loads(self.source_raw)
            except json.JSONDecodeError as e:
                self._report_error(e)
                self.harvest_source.update_job_record_count_by_action("errored")
                return False
        else:
            record = self.transformed_data

        # save ourselves a second call to is_valid by keeping a flag of
        # whether we saw any errors
        valid = True

        errors = self.harvest_source.validator.iter_errors(record)
        errors = assemble_validation_errors(errors)
        for error in errors:
            valid = False
            self._report_error(error)

        if valid:
            self.harvest_source.update_job_record_count_by_action("validated")
            return True
        else:
            # update the reporter only once even with multiple errors
            self.harvest_source.update_job_record_count_by_action("errored")
            return False

    def sync(self):
        try:
            if self.status == "error":
                return False
            if ckan_sync_tool.sync(record=self) is True:
                self.harvest_source.update_job_record_count_by_action(self.action)
                self.update_self_in_db()
                return True
        except (
            DCATUSToCKANException,
            SynchronizeException,
            CKANDownException,
            CKANRejectionException,
        ):
            self.status = "error"
            self.harvest_source.update_job_record_count_by_action("errored")
        return False

    def update_self_in_db(self) -> bool:
        data = {
            "status": self.status,
            "date_finished": get_datetime(),
        }
        if self.ckan_id is not None:
            data["ckan_id"] = self.ckan_id
        if self.ckan_name is not None:
            data["ckan_name"] = self.ckan_name

        if self.harvest_source.job_type == "force_harvest":
            data["harvest_job_id"] = self.harvest_source.job_id

        self.harvest_source.db_interface.update_harvest_record(
            self.id,
            data,
        )


def harvest_job_starter(job_id, job_type="harvest"):
    logger.info(f"Harvest job starting for JobId: {job_id}")
    harvest_source = HarvestSource(job_id, job_type)

    # Check if another job is already in progress for this source
    jobs = harvest_source.db_interface.get_in_progress_jobs()
    for job in jobs:
        if job.harvest_source_id == harvest_source.id and job.id != job_id:
            logger.error(
                f"Job {job.id} is already in progress for source {harvest_source.name}. Exiting."
            )
            harvest_source.finish_job_with_status("error")
            return
    # Check if another task is already running this job
    handler = CFHandler(CF_API_URL, CF_SERVICE_USER, CF_SERVICE_AUTH)
    running_tasks = handler.get_running_app_tasks()
    running_harvest_ids = handler.job_ids_from_tasks(running_tasks)
    if isinstance(running_harvest_ids, list) and running_harvest_ids.count(job_id) > 1:
        logger.error(f"Job {job_id} is already running in another task. Exiting.")
        # Don't finish the job here, just exit to prevent duplicate processing
        return

    if job_type in ["harvest", "force_harvest", "clear"]:
        harvest_source.run_full_harvest()

    if job_type == "validate":
        harvest_source.acquire_minimum_external_data()
        for record in harvest_source.external_records_to_process():
            if harvest_source.schema_type.startswith("iso19115"):
                record.transform()
            try:
                record.validate()
            except:  # noqa: E722
                pass

    # generate harvest job report
    harvest_source.report()

    logger.info(f"Harvest job completed for JobId: {job_id}")

    # close the db connection after job to prevent persistent open connections
    harvest_source.db_interface.close()

    # close the connection to RemoteCKAN
    ckan_sync_tool.close_conection()


def check_for_more_work():
    """Call back to the load manager to start new tasks.

    At the end of the harvest job, look for whether there are still new
    jobs to be done and schedule at most one new task.
    """
    try:
        LoadManager()._start_new_jobs(check_from_task=True)
    except Exception as e:
        logger.error(f"Error checking for more work: {repr(e)}")
        # We don't want to raise here, just log the error and continue
        # This is to ensure that the harvest job can finish gracefully
        # even if there is an issue with checking for more work.
        # The application should pick up jobs every 15 minutes,
        # this is only for speed.
        return


if __name__ == "__main__":
    import sys

    from harvester.utils.general_utils import parse_args

    exit_code = 0

    try:
        args = parse_args(sys.argv[1:])
        harvest_job_starter(args.jobId, args.jobType)
    except Exception as e:
        logger.error(f"Harvest has experienced an error :: {repr(e)}")
        exit_code = 1
    finally:
        check_for_more_work()
        sys.exit(exit_code)
