import os
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple

import click
import requests
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

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
        self.headers = {
            "Authorization": f"Bearer {CKAN_API_TOKEN}",
            "Content-Type": "application/json",
        }
        session = create_retry_session()
        self.ckan_tool = CKANSyncTool(session=session)

    def fetch_all_ckan_records(self) -> List[Dict[str, Any]]:
        """
        Fetch all records from CKAN SOLR using pagination.
        """
        all_records = []
        start = 0

        click.echo("Fetching records from CKAN SOLR...")

        while True:
            try:
                response = requests.get(
                    f"{self.ckan_api_url}/api/action/package_search",
                    params={
                        "rows": self.batch_size,
                        "start": start,
                        "fq": "(include_collection:true)",
                        "fl": "id,extras_identifier,extras_harvest_object_id",
                    },
                    headers=self.headers,
                    timeout=30,
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

    def get_db_harvest_object_ids(self) -> Set[str]:
        """
        Get all harvest_object_ids from the database.
        """
        click.echo("Fetching harvest_object_ids from database...")

        # Fetch all harvest_object_ids from DB in chunks to handle large datasets
        all_ids = set()
        offset = 0

        while True:
            stmt = (
                select(HarvestRecord.id)
                .where(HarvestRecord.id.isnot(None))
                .offset(offset)
                .limit(self.batch_size)
            )

            result = self.db_interface.db.execute(stmt)
            batch_ids = [row[0] for row in result.fetchall()]

            if not batch_ids:
                break

            all_ids.update(batch_ids)
            offset += len(batch_ids)

            click.echo(f"Loaded {len(all_ids)} harvest_object_ids from DB...")

        click.echo(f"Total harvest_object_ids in DB: {len(all_ids)}")
        return all_ids

    def get_db_identifiers(self) -> Set[str]:
        """
        Get all identifiers from the database.
        """
        click.echo("Fetching identifiers from database...")

        all_identifiers = set()
        offset = 0

        while True:
            stmt = (
                select(HarvestRecord.identifier)
                .where(HarvestRecord.identifier.isnot(None))
                .offset(offset)
                .limit(self.batch_size)
            )

            result = self.db_interface.db.execute(stmt)
            batch_identifiers = [row[0] for row in result.fetchall()]

            if not batch_identifiers:
                break

            all_identifiers.update(batch_identifiers)
            offset += len(batch_identifiers)

            click.echo(f"Loaded {len(all_identifiers)} identifiers from DB...")

        click.echo(f"Total identifiers in DB: {len(all_identifiers)}")
        return all_identifiers

    def categorize_ckan_records(
        self,
        ckan_records: List[Dict[str, Any]],
        db_harvest_ids: Set[str],
        db_identifiers: Set[str],
    ) -> Tuple[List[Dict], List[Dict], List[Dict], List[str]]:
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
        # if identifier only exists in db than it needs to be added to ckan solr
        to_add = db_identifiers.copy()

        for record in ckan_records:
            harvest_id = record.get("harvest_object_id")
            identifier = record.get("identifier")
            # If harvest_object_id exists in DB, already synced
            if harvest_id and harvest_id in db_harvest_ids:
                synced.append(record)
                # if it's in we don't need to add it.
                to_add.discard(identifier)
            # If identifier is in the db identifiers we can remove the identifier
            # from the to_add set, as it means we have a record.
            elif identifier and identifier in db_identifiers:
                # discard is the safe method
                to_add.discard(identifier)
            # delete if we don't have the identifier in the db
            elif identifier and identifier not in db_identifiers:
                to_delete.append(record)
            # If it doesn't fit any of the above, it's ppossible we need to update
            else:
                to_update.append(record)
        return synced, to_update, to_delete, to_add

    def get_db_record_by_identifier(self, identifier: str) -> Optional[HarvestRecord]:
        """
        Get database record by identifier using the DB interface.
        """
        stmt = select(HarvestRecord).where(HarvestRecord.identifier == identifier)
        result = self.db_interface.db.execute(stmt)
        return result.scalar_one_or_none()

    def update_ckan_record(self, record: Dict[str, Any]) -> bool:
        """
        Update a CKAN record to link it with the database record.
        """
        try:
            # Get the corresponding DB record to get the proper harvest_object_id
            db_record = self.get_db_record_by_identifier(record["identifier"])

            if not db_record:
                click.echo(
                    "Warning: DB record not found for identifier "
                    f"{record['identifier']}"
                )
                return False

            # Prepare update data
            update_data = {
                "id": record["id"],
                "extras_harvest_object_id": db_record.harvest_object_id,
            }

            response = requests.post(
                f"{self.ckan_api_url}/api/action/package_patch",
                json=update_data,
                headers=self.headers,
                timeout=30,
            )
            response.raise_for_status()

            result = response.json()
            if result.get("success"):
                return True
            else:
                click.echo(
                    f"CKAN update failed for {record['id']}: {result.get('error')}"
                )
                return False

        except Exception as e:
            click.echo(f"Error updating CKAN record {record['id']}: {e}")
            return False

    def delete_ckan_record(self, record: Dict[str, Any]) -> bool:
        """
        Delete a CKAN record that doesn't exist in the database.
        """
        try:
            response = requests.post(
                f"{self.ckan_api_url}/api/action/package_delete",
                json={"id": record["id"]},
                headers=self.headers,
                timeout=30,
            )
            response.raise_for_status()

            result = response.json()
            if result.get("success"):
                return True
            else:
                click.echo(
                    f"CKAN deletion failed for {record['id']}: {result.get('error')}"
                )
                return False

        except Exception as e:
            click.echo(f"Error deleting CKAN record {record['id']}: {e}")
            return False

    def process_updates_batch(self, records_to_update: List[Dict]) -> Tuple[int, int]:
        """Process record updates in batches with progress tracking."""
        success_count = 0
        failure_count = 0

        click.echo(f"Updating {len(records_to_update)} CKAN records...")

        with click.progressbar(records_to_update, label="Updating records") as bar:
            for record in bar:
                if self.update_ckan_record(record):
                    success_count += 1
                else:
                    failure_count += 1

                # Small delay to avoid overwhelming the API
                time.sleep(0.1)

        return success_count, failure_count

    def process_deletions_batch(self, records_to_delete: List[Dict]) -> Tuple[int, int]:
        """Process record deletions in batches with progress tracking."""
        success_count = 0
        failure_count = 0

        click.echo(f"Deleting {len(records_to_delete)} CKAN records...")

        with click.progressbar(records_to_delete, label="Deleting records") as bar:
            for record in bar:
                if self.delete_ckan_record(record):
                    success_count += 1
                else:
                    failure_count += 1

                # Small delay to avoid overwhelming the API
                time.sleep(0.1)

        return success_count, failure_count

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

        try:
            # Step 1: Fetch all data
            ckan_records = self.fetch_all_ckan_records()
            db_harvest_ids = self.get_db_harvest_object_ids()
            db_identifiers = self.get_db_identifiers()
            stats.total_ckan_records = len(ckan_records)

            # Step 2: Categorize records
            synced, to_update, to_delete, to_add = self.categorize_ckan_records(
                ckan_records, db_harvest_ids, db_identifiers
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
            click.echo(f"Total DB records: {self.get_db_record_count()}")
            click.echo(f"Already synced: {stats.already_synced}")
            click.echo(f"Need update: {stats.to_update}")
            click.echo(f"Need deletion: {stats.to_delete}")
            click.echo(f"Need addition: {stats.to_add}")
            if dry_run:
                click.echo("\nDRY RUN - No changes will be made")
                return stats

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
                for identifier in to_add:
                    try:
                        record = self.get_db_record_by_identifier(identifier)
                        # Create a new record in CKAN for the identifier
                        self.ckan_tool.add_record(record)
                        stats.add_success += 1
                    except Exception as e:
                        error_msg = f"Failed to add identifier {identifier}: {e}"
                        stats.errors.append(error_msg)
                        click.echo(f"ERROR: {error_msg}")
                        stats.add_failed += 1

        except Exception as e:
            error_msg = f"Synchronization failed: {e}"
            stats.errors.append(error_msg)
            click.echo(f"ERROR: {error_msg}")

        return stats

    def get_sync_preview(self) -> Dict[str, List[Dict]]:
        """
        Get a preview of what the synchronization would do without making changes.

        Dictionary with categorized records for preview
        """
        ckan_records = self.fetch_all_ckan_records()
        db_harvest_ids = self.get_db_harvest_object_ids()
        db_identifiers = self.get_db_identifiers()

        synced, to_update, to_delete, to_add = self.categorize_ckan_records(
            ckan_records, db_harvest_ids, db_identifiers
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
@click.option(
    "--preview", is_flag=True, help="Show detailed preview of changes", default=True
)
def sync_command(batch_size, dry_run, preview):
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

        if preview:
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
                        f"  - CKAN ID: {record['id']}, Identifier: "
                        f"{record['identifier']}"
                    )

            if preview_data["to_delete"]:
                click.echo("\nFirst 5 records to DELETE:")
                for record in preview_data["to_delete"][:5]:
                    click.echo(
                        f"  - CKAN ID: {record['id']}, "
                        f"Identifier: {record.get('identifier', 'N/A')}"
                    )

        if not dry_run:
            # Perform synchronization
            stats = sync_manager.synchronize(dry_run=dry_run)

            # Print results
            print_sync_results(stats)


if __name__ == "__main__":
    sync_command()
