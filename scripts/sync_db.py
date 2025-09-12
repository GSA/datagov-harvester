import csv
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, NamedTuple, Optional, Tuple, Union

import click
import requests
from sqlalchemy import create_engine, desc, func, select
from sqlalchemy.orm import aliased, sessionmaker

from harvester.utils.ckan_utils import CKANSyncTool
from harvester.utils.general_utils import create_retry_session

# because this runs outside of the context the flask app
# we need this line to help resolve the python path
# to find the harvester modules
sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))
from database.models import HarvestRecord
from harvester import HarvesterDBInterface

DATABASE_URI = os.getenv("DATABASE_URI")
CKAN_API_TOKEN = os.getenv("CKAN_API_TOKEN")
CKAN_API_URL = os.getenv("CKAN_API_URL")


@dataclass
class SyncStats:
    """Statistics for the synchronization process."""

    total_ckan_records: int = 0
    already_synced: int = 0
    to_update: int = 0
    to_delete: int = 0
    to_add: int = 0
    updated_success: int = 0
    updated_failed: int = 0
    deleted_success: int = 0
    deleted_failed: int = 0
    add_success: int = 0
    add_failed: int = 0
    errors: List[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


@dataclass
class SyncResult:
    """Represents a sync operation result for CSV reporting."""

    identifier: str
    harvest_source_id: str
    ckan_id: str
    ckan_name: str
    action: str  # 'synced', 'to_update', 'to_add', 'to_delete'
    success: bool
    error_message: str = ""


class CategorizedRecords(NamedTuple):
    """
    Represents categorized records for the steps of the sync process.

    synced: List of HarvestRecord objects that are already synced.
    to_update: List of HarvestRecord objects that need to be updated.
    to_delete: List of HarvestRecord objects or CKAN record dicts to be deleted.
    to_add: List of HarvestRecord objects that need to be added.
    """

    synced: List[HarvestRecord]
    to_update: List[HarvestRecord]
    to_delete: List[Union[Dict, HarvestRecord]]
    to_add: List[HarvestRecord]


class CKANSyncManager:
    """Manages synchronization between database and CKAN SOLR."""

    def __init__(
        self,
        db_interface: HarvesterDBInterface,
        batch_size: int = 1000,
    ):
        self.ckan_api_url = CKAN_API_URL
        self.ckan_api_key = CKAN_API_TOKEN
        self.db_interface = db_interface
        self.batch_size = batch_size
        self.session = create_retry_session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {CKAN_API_TOKEN}",
                "Content-Type": "application/json",
            }
        )
        self.ckan_tool = CKANSyncTool(session=self.session)
        self.sync_results: List[SyncResult] = []

    def fetch_all_ckan_records(self) -> List[Dict[str, Any]]:
        """
        Fetch all records from CKAN SOLR using pagination.
        """
        all_records = []
        start = 0

        click.echo("Fetching records from CKAN SOLR...")

        while True:
            try:
                response = self.session.get(
                    f"{self.ckan_api_url}/api/action/package_search",
                    params={
                        "rows": self.batch_size,
                        "start": start,
                        "fq": "(include_collection:true)",
                        "fl": "id,extras_identifier,extras_harvest_object_id,"
                        "extras_harvest_source_title,metadata_modified,name",
                    },
                )
                response.raise_for_status()

                data = response.json()
                if not data.get("success"):
                    raise ValueError(
                        f"CKAN API error: {data.get('error', 'Unknown error')}"
                    )

                result = data["result"]
                records = result.get("results", [])
                if not records:
                    break

                all_records.extend(records)

                start += len(records)
                click.echo(f"Fetched {len(all_records)} CKAN records so far...")

                # Break if we got fewer records than requested (end of data)
                if len(records) < self.batch_size:
                    break

            except requests.RequestException as e:
                click.echo(f"Error fetching CKAN data: {e}")
                raise

        click.echo(f"Total CKAN records fetched: {len(all_records)}")
        return all_records

    def get_latest_db_records(self) -> Dict[str, HarvestRecord]:
        """
        Get all latest records from the database.
        Code based off:
        get_latest_harvest_records_by_source_orm
        """
        click.echo("Fetching latest records from database...")

        queries = [
            HarvestRecord.status == "success",
        ]

        subq = (
            self.db_interface.db.query(HarvestRecord)
            .filter(*queries)
            .order_by(
                HarvestRecord.identifier,
                HarvestRecord.harvest_source_id,
                desc(HarvestRecord.date_created),
            )
            .distinct(HarvestRecord.identifier, HarvestRecord.harvest_source_id)
            .subquery()
        )

        sq_alias = aliased(HarvestRecord, subq)
        result = list(self.db_interface.db.query(sq_alias).all())
        click.echo(f"Total latest records from DB: {len(result)}")
        return {record.identifier: record for record in result}

    def is_within_5_minutes(self, datetime_str: str, datetime_obj: datetime) -> bool:
        """
        Safe version that handles naive datetime objects by assuming UTC.

        datetime_str (str): ISO format datetime string
        datetime_obj (datetime): Datetime object (naive or aware)
        """
        try:
            # Parse the ISO format string
            parsed_dt = datetime.fromisoformat(datetime_str.replace("Z", "+00:00"))

            # Handle naive datetime_obj by assuming it's UTC
            if datetime_obj.tzinfo is None:
                datetime_obj = datetime_obj.replace(tzinfo=timezone.utc)

            # Calculate absolute time difference
            time_diff = abs((parsed_dt - datetime_obj).total_seconds())
            return time_diff <= 300

        except (ValueError, AttributeError):
            return False

    def needs_update(self, record: Dict[str, Any], db_record: HarvestRecord) -> bool:
        """
        Check if a record needs to be updated.
        Based on either name change or if the record is within 5 minutes
        of the last update.
        """
        if record.get("name") != db_record.ckan_name or self.is_within_5_minutes(
            record.get("metadata_modified"), db_record.date_finished
        ):
            return True
        return False

    def categorize_ckan_records(
        self,
        ckan_records: List[Dict[str, Any]],
        db_records: Dict[str, HarvestRecord],
    ) -> CategorizedRecords:
        """
        Categorize CKAN records based on sync requirements.

        ckan_records: All records from CKAN
        db_harvest_ids: Set of harvest_object_ids in database
        db_identifiers: Set of identifiers in database

        provides a tuple of
        (synced, to_update, to_delete records and to_add identifiers)
        """
        synced = []  # harvest_object_id exists in DB - already synced
        to_update = []  # identifier exists in DB but harvest_object_id doesn't
        to_delete = []  # neither identifier nor harvest_object_id exists in DB
        processed_identifiers = set()

        for record in ckan_records:
            identifier = record.get("identifier")
            db_record = db_records.get(identifier)

            # if the db record no longer exists because it was
            # cleared with script it should be deleted, or if it was marked for
            # deletion in the database we should delete it from CKAN
            if not db_record or (db_record.action == "delete"):
                to_delete.append(db_record if db_record else record)
            # if the current harveest_object_id matches the db record
            # then it is already synced with the latest entry in the db
            elif record.get("harvest_object_id") == db_record.id:
                synced.append(db_record)
                processed_identifiers.add(identifier)
            # lastly we check for any differences such as name or if the
            # age of the record may be too different we would want to update
            # the record in CKAN to match the latest entry in the database
            elif self.needs_update(record, db_record):
                to_update.append(db_record)
                processed_identifiers.add(identifier)

        # if the identifier is not in the processed identifiers
        # then it is a new record that needs to be added to CKAN
        to_add = [
            db_record
            for identifier, db_record in db_records.items()
            if identifier not in processed_identifiers
        ]

        return CategorizedRecords(synced, to_update, to_delete, to_add)

    def get_db_record_by_identifier(self, identifier: str) -> Optional[HarvestRecord]:
        """
        Get database record by identifier using the DB interface.
        """
        stmt = select(HarvestRecord).where(HarvestRecord.identifier == identifier)
        result = self.db_interface.db.execute(stmt)
        return result.scalar_one_or_none()

    def sync_record(self, db_record: HarvestRecord) -> bool:
        """
        create, update, delete a CKAN..
        """
        try:
            # returns True if successful, or goes to an exception
            return self.ckan_tool.sync(db_record)

        except Exception as e:
            click.echo(f"Error updating CKAN record identifier:{db_record}: {e}")
            return False

    def _create_sync_result(
        self,
        record: Union[HarvestRecord, Dict],
        action: str,
        success: bool,
        error_message: str = "",
    ) -> SyncResult:
        """Create a SyncResult object from a record."""
        if isinstance(record, HarvestRecord):
            return SyncResult(
                identifier=record.identifier,
                harvest_source_id=record.harvest_source_id,
                ckan_id=record.ckan_id or "",
                ckan_name=record.ckan_name or "",
                action=action,
                success=success,
                error_message=error_message,
            )
        else:
            # Handle CKAN record dict for deletions
            return SyncResult(
                identifier=record.get("identifier", ""),
                harvest_source_id="",  # Not available in CKAN record
                ckan_id=record.get("id", ""),
                ckan_name=record.get("name", ""),
                action=action,
                success=success,
                error_message=error_message,
            )

    def process_updates_batch(
        self, records_to_update: List[HarvestRecord]
    ) -> Tuple[int, int]:
        """Process record updates in batches with progress tracking."""
        success_count = 0
        failure_count = 0

        click.echo(f"Updating {len(records_to_update)} CKAN records...")

        with click.progressbar(records_to_update, label="Updating records") as bar:
            for record in bar:
                try:
                    if self.sync_record(record):
                        success_count += 1
                        self.sync_results.append(
                            self._create_sync_result(record, "to_update", True)
                        )
                    else:
                        failure_count += 1
                        self.sync_results.append(
                            self._create_sync_result(
                                record, "to_update", False, "Sync operation failed"
                            )
                        )
                except Exception as e:
                    failure_count += 1
                    self.sync_results.append(
                        self._create_sync_result(record, "to_update", False, str(e))
                    )

        return success_count, failure_count

    def process_deletions_batch(
        self, records_to_delete: List[Union[Dict, HarvestRecord]]
    ) -> Tuple[int, int]:
        """
        Process record deletions in batches with progress tracking.
        Deletes using either HarvestRecord object or CKAN record dict.
        """
        success_count = 0
        failure_count = 0

        click.echo(f"Deleting {len(records_to_delete)} CKAN records...")

        with click.progressbar(records_to_delete, label="Deleting records") as bar:
            for record in bar:
                try:
                    if isinstance(record, dict):
                        self.ckan_tool.ckan.action.dataset_purge(id=record["id"])
                        success_count += 1
                        self.sync_results.append(
                            self._create_sync_result(record, "to_delete", True)
                        )
                    elif isinstance(record, HarvestRecord):
                        if self.sync_record(record):
                            success_count += 1
                            self.sync_results.append(
                                self._create_sync_result(record, "to_delete", True)
                            )
                        else:
                            failure_count += 1
                            self.sync_results.append(
                                self._create_sync_result(
                                    record, "to_delete", False, "Sync operation failed"
                                )
                            )
                    else:
                        failure_count += 1
                        # Create a generic sync result for unknown record types
                        self.sync_results.append(
                            SyncResult(
                                "unknown",
                                "",
                                "",
                                "",
                                "to_delete",
                                False,
                                "Unknown record type",
                            )
                        )
                except Exception as e:
                    failure_count += 1
                    self.sync_results.append(
                        self._create_sync_result(record, "to_delete", False, str(e))
                    )

        return success_count, failure_count

    def process_additions_batch(
        self, records_to_add: List[HarvestRecord]
    ) -> Tuple[int, int]:
        """Process record additions in batches with progress tracking."""
        success_count = 0
        failure_count = 0

        click.echo(f"Adding {len(records_to_add)} CKAN records...")

        with click.progressbar(records_to_add, label="Adding records") as bar:
            for record in bar:
                try:
                    if self.sync_record(record):
                        success_count += 1
                        self.sync_results.append(
                            self._create_sync_result(record, "to_add", True)
                        )
                    else:
                        failure_count += 1
                        self.sync_results.append(
                            self._create_sync_result(
                                record, "to_add", False, "Sync operation failed"
                            )
                        )
                except Exception as e:
                    failure_count += 1
                    self.sync_results.append(
                        self._create_sync_result(record, "to_add", False, str(e))
                    )

        return success_count, failure_count

    def _record_synced_items(self, synced_records: List[HarvestRecord]):
        """Record already synced items for CSV reporting."""
        for record in synced_records:
            self.sync_results.append(self._create_sync_result(record, "synced", True))

    def write_csv_report(self):
        """Write sync results to CSV file."""
        if not self.sync_results:
            click.echo("No sync results to write to CSV.")
            return

        try:
            with open("sync_report.csv", "w", newline="", encoding="utf-8") as csvfile:
                fieldnames = [
                    "identifier",
                    "harvest_source_id",
                    "ckan_id",
                    "ckan_name",
                    "action",
                    "success",
                    "error_message",
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                writer.writeheader()
                for result in self.sync_results:
                    writer.writerow(
                        {
                            "identifier": result.identifier,
                            "harvest_source_id": result.harvest_source_id,
                            "ckan_id": result.ckan_id,
                            "ckan_name": result.ckan_name,
                            "action": result.action,
                            "success": result.success,
                            "error_message": result.error_message,
                        }
                    )

            click.echo("CSV report written to: sync_report.csv")
            click.echo(f"Total records in report: {len(self.sync_results)}")

        except Exception as e:
            click.echo(f"Error writing CSV file: {e}")

    def get_db_record_count(self) -> int:
        """Get total count of harvest records in database."""
        stmt = select(func.count(HarvestRecord.id))
        result = self.db_interface.db.execute(stmt)
        return result.scalar()

    def synchronize(self, dry_run: bool = True) -> SyncStats:
        """
        Perform full synchronization between database and CKAN SOLR.
        """
        stats = SyncStats()
        self.sync_results = []  # Reset results for each sync

        try:
            # Step 1: Fetch all data
            db_records = self.get_latest_db_records()
            ckan_records = self.fetch_all_ckan_records()
            stats.total_ckan_records = len(ckan_records)

            # Step 2: Categorize records
            synced, to_update, to_delete, to_add = self.categorize_ckan_records(
                ckan_records, db_records
            )

            stats.already_synced = len(synced)
            stats.to_update = len(to_update)
            stats.to_delete = len(to_delete)
            stats.to_add = len(to_add)

            # Step 3: Display analysis
            click.echo("\n" + "=" * 60)
            click.echo("SYNCHRONIZATION ANALYSIS")
            click.echo("=" * 60)
            click.echo(f"Total CKAN records: {stats.total_ckan_records}")
            click.echo(f"Latest DB records: {len(db_records)}")
            click.echo(f"Already synced: {stats.already_synced}")
            click.echo(f"Need update: {stats.to_update}")
            click.echo(f"Need deletion: {stats.to_delete}")
            click.echo(f"Need addition: {stats.to_add}")

            # Record synced items for CSV reporting
            self._record_synced_items(synced)

            if dry_run:
                click.echo("\nDRY RUN - No changes will be made")
                # In dry run, record planned actions without success status
                for record in to_update:
                    self.sync_results.append(
                        self._create_sync_result(
                            record,
                            "to_update_planned",
                            True,
                            "Dry run - no action taken",
                        )
                    )
                for record in to_add:
                    self.sync_results.append(
                        self._create_sync_result(
                            record, "to_add_planned", True, "Dry run - no action taken"
                        )
                    )
                for record in to_delete:
                    self.sync_results.append(
                        self._create_sync_result(
                            record,
                            "to_delete_planned",
                            True,
                            "Dry run - no action taken",
                        )
                    )
            else:
                # Step 4: Process updates
                if to_update:
                    success, failed = self.process_updates_batch(to_update)
                    stats.updated_success = success
                    stats.updated_failed = failed

                # Step 5: Process deletions
                if to_delete:
                    success, failed = self.process_deletions_batch(to_delete)
                    stats.deleted_success = success
                    stats.deleted_failed = failed

                # Step 6: Handle additions
                if to_add:
                    success, failed = self.process_additions_batch(to_add)
                    stats.add_success = success
                    stats.add_failed = failed

            # Write CSV report if requested
            self.write_csv_report()

        except Exception as e:
            error_msg = f"Synchronization failed: {e}"
            stats.errors.append(error_msg)
            click.echo(f"ERROR: {error_msg}")

        return stats

    def get_sync_preview(self) -> Dict[str, List[Dict]]:
        """
        Get a preview of what the synchronization would do without making changes.
        """
        db_records = self.get_latest_db_records()
        ckan_records = self.fetch_all_ckan_records()

        synced, to_update, to_delete, to_add = self.categorize_ckan_records(
            ckan_records, db_records
        )

        return {
            "synced": synced,
            "to_update": to_update,
            "to_delete": to_delete,
            "summary": {
                "total_ckan": len(ckan_records),
                "total_db": self.get_db_record_count(),
                "already_synced": len(synced),
                "need_update": len(to_update),
                "need_deletion": len(to_delete),
                "need_added": len(to_add),
            },
        }


def print_sync_results(stats: SyncStats):
    """Print detailed synchronization results."""
    click.echo("\n" + "=" * 60)
    click.echo("SYNCHRONIZATION RESULTS")
    click.echo("=" * 60)
    click.echo(f"Total CKAN records processed: {stats.total_ckan_records}")
    click.echo(f"Already synced: {stats.already_synced}")
    click.echo(f"Updates attempted: {stats.to_update}")
    click.echo(f"  - Successful: {stats.updated_success}")
    click.echo(f"  - Failed: {stats.updated_failed}")
    click.echo(f"Deletions attempted: {stats.to_delete}")
    click.echo(f"  - Successful: {stats.deleted_success}")
    click.echo(f"  - Failed: {stats.deleted_failed}")

    if stats.errors:
        click.echo(f"\nErrors encountered: {len(stats.errors)}")
        for error in stats.errors:
            click.echo(f"  - {error}")

    total_changes = stats.updated_success + stats.deleted_success
    click.echo(f"\nTotal successful changes: {total_changes}")


@click.command()
@click.option("--batch-size", default=1000, help="Batch size for processing (<=1000)")
@click.option(
    "--dry-run", is_flag=True, help="Analyze only, make no changes", default=True
)
def sync_command(batch_size, dry_run):
    """
    Synchronize database records with CKAN SOLR.

    The database is treated as the source of truth. CKAN records will be
    updated or deleted based on what exists in the database.
    """
    # short-circuit if the batch size is too large or not int
    if not isinstance(batch_size, int) or batch_size > 1000:
        click.echo("Batch size must be 1000 or less or not int.")
        sys.exit(1)
        raise ValueError("Batch size must be 1000 or less or not value is not an int.")

    # Setup database connection
    engine = create_engine(DATABASE_URI)
    SessionLocal = sessionmaker(bind=engine)

    with SessionLocal() as session:
        # Create DB interface
        db_interface = HarvesterDBInterface(session)

        # Initialize sync manager
        sync_manager = CKANSyncManager(
            db_interface=db_interface,
            batch_size=batch_size,
        )

        # Show detailed preview
        preview_data = sync_manager.get_sync_preview()

        click.echo("\n" + "=" * 60)
        click.echo("DETAILED SYNC PREVIEW")
        click.echo("=" * 60)

        summary = preview_data["summary"]
        click.echo(f"Total CKAN records: {summary['total_ckan']}")
        click.echo(f"Total DB records: {summary['total_db']}")
        click.echo(f"Already synced: {summary['already_synced']}")
        click.echo(f"Need update: {summary['need_update']}")
        click.echo(f"Need deletion: {summary['need_deletion']}")
        click.echo(f"Need to be added: {summary['need_added']}")

        if preview_data["to_update"]:
            click.echo("\nFirst 5 records to UPDATE:")
            for record in preview_data["to_update"][:5]:
                click.echo(
                    f"  - CKAN ID: {record.ckan_id}, Identifier: "
                    f"{record.identifier}"
                )
        if preview_data["to_delete"]:
            click.echo("\nFirst 5 records to DELETE:")
            for record in preview_data["to_delete"][:5]:
                if isinstance(record, dict):
                    click.echo(
                        f"  - CKAN ID: {record['id']}, "
                        f"Identifier: {record.get('identifier', 'N/A')}"
                    )
                else:
                    click.echo(
                        f"  - CKAN ID: {record.ckan_id}, "
                        f"Identifier: {record.identifier}"
                    )

        # Perform synchronization
        stats = sync_manager.synchronize(dry_run=dry_run)
        if not dry_run:
            # Print results of the synchronization
            print_sync_results(stats)


if __name__ == "__main__":
    sync_command()
