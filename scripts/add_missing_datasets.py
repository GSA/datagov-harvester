from sqlalchemy import create_engine, desc, exists
from sqlalchemy.orm import scoped_session, sessionmaker, aliased
import sys
import os
import logging
import json

sys.path.insert(1, "/".join(os.path.realpath(__file__).split("/")[0:-2]))

from database.models import HarvestRecord, Dataset, Organization, HarvestSource
from harvester import HarvesterDBInterface  # noqa E402
from harvester.utils.general_utils import (
    add_uuid_to_package_name,
    munge_title_to_name,
    translate_spatial_to_geojson,
)


DATABASE_URI = os.getenv("DATABASE_URI")

# create a scopedsession for our harvest runner
engine = create_engine(DATABASE_URI)
session_factory = sessionmaker(bind=engine, autoflush=True)
session = scoped_session(session_factory)

logger = logging.getLogger("add_missing_dataset")


def guarantee_slug(slug, interface, max_attempts=10):
    """
    guarantees slug uniqueness by recursively calculating until
    max_attempts.
    """
    output = slug
    attempts = 0

    while attempts < max_attempts:
        if not interface.get_dataset_by_slug(output):
            return output
        output = add_uuid_to_package_name(slug)
        attempts += 1


def determine_slug(metadata, interface, harvest_record):
    """
    slug creation scenarios
        create (never got to dataset)
        update (update never happened but previous record in dataset)
        update (never got to dataset)
    """
    slug = munge_title_to_name(metadata["title"])

    dataset = interface.get_dataset_by_slug(slug)

    if dataset:
        if (
            dataset.dcat["title"] != metadata["title"]
            or harvest_record.action == "create"
        ):
            slug = guarantee_slug(slug, interface)

    return slug


def get_missing_datasets(interface):
    """
    gets all the harvest records which aren't in the dataset table but should be
    """
    subq = (
        interface.db.query(HarvestRecord)
        .filter(HarvestRecord.status == "success")
        .order_by(
            HarvestRecord.identifier,
            HarvestRecord.harvest_source_id,
            desc(HarvestRecord.date_created),
        )
        .distinct(HarvestRecord.identifier, HarvestRecord.harvest_source_id)
        .subquery()
    )

    sq_alias = aliased(HarvestRecord, subq)

    missing_datasets = (
        interface.db.query(sq_alias)
        .filter(sq_alias.action != "delete")
        .filter(~exists().where(Dataset.harvest_record_id == sq_alias.id))
    )
    return missing_datasets.count(), missing_datasets.yield_per(500)


def get_org_from_record(harvest_record, interface):
    """
    there's no backref so need to do it the hard way
    """
    return (
        interface.db.query(Organization)
        .join(HarvestSource, HarvestSource.organization_id == Organization.id)
        .filter(HarvestSource.id == harvest_record.harvest_source_id)
        .one_or_none()
    )


def get_dcat_metadata(harvest_record):
    try:
        return (
            harvest_record.source_transform
            if harvest_record.source_transform
            else json.loads(harvest_record.source_raw)
        )
    except json.decoder.JSONDecodeError:
        return {}


def record_to_dataset(harvest_record, interface):
    """
    transforms the harvest record into a dataset payload
    """

    metadata = get_dcat_metadata(harvest_record)

    if not metadata:
        logger.warning("can't retrieve dcatus metadata")
        return

    slug = create_slug(metadata, interface, harvest_record)

    org = get_org_from_record(harvest_record, interface)

    if not org:
        logger.warning(
            f"organization for the harvest record {harvest_record.id} doesn't exist. "
            "skipping dataset"
        )
        return

    dataset_payload = {
        "slug": slug,
        "dcat": metadata,
        "organization_id": get_org_from_record(harvest_record).id,
        "harvest_source_id": harvest_record.harvest_source_id,
        "harvest_record_id": harvest_record.id,
        "last_harvested_date": harvest_record.date_finished,
    }

    try:
        dataset_payload["translated_spatial"] = translate_spatial_to_geojson(
            metadata.get("spatial")
        )
    except Exception as e:
        logger.warning(f"unable to translate spatial value: {e}")

    return dataset_payload


def main():
    """
    harvest records were missing from the dataset table. this script adds them.
    https://github.com/GSA/data.gov/issues/5883

    at the time of making this 0 "deleted" harvest records were in the dataset table so
    this tool doesn't handle deletions only updates and creations
    """

    interface = HarvesterDBInterface(session=session)

    logger.info("retrieving missing datasets...")
    missing_datasets_count, missing_datasets = get_missing_datasets(interface)

    if missing_datasets_count == 0:
        logger.info("no missing datasets from the harvest record table. exiting")
        return

    success, failure = 0, 0

    logger.info(f"processing {missing_datasets_count} missing datasets...")
    for harvest_record in missing_datasets:
        try:
            dataset_payload = record_to_dataset(harvest_record, interface)
            if not dataset_payload:
                logger.warning(
                    "unable to create dataset payload for harvest "
                    f"record {harvest_record.id}"
                )
                failure += 1
                continue

            dataset = interface.upsert_dataset(dataset_payload)

            if dataset:
                logger.info(f"{harvest_record.action}d dataset '{dataset.slug}'")
                success += 1
        except Exception as e:
            logger.warning(
                "exception occurred when attempting to "
                f"fix missing harvest record {harvest_record.id} : "
                f"{str(e)}"
            )
            failure += 1

    logger.info(
        f"done processing missing harvest records. added: {success} failed: {failure}"
    )


if __name__ == "__main__":
    main()
