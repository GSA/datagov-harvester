from datetime import datetime, timezone

from database.models import DatasetViewCount
from tests.conftest import create_record_for_dataset, dataset_payload


def test_insert_dataset_view_counts(interface, view_count_datasets):
    # add record to show delete
    dataset_view_count = DatasetViewCount(dataset_slug="a", view_count=1)
    interface.db.add(dataset_view_count)
    interface.db.commit()

    datasets = interface.db.query(DatasetViewCount).all()
    assert len(datasets) == 1

    interface.insert_view_counts_of_datasets(view_count_datasets)

    datasets = interface.db.query(DatasetViewCount).all()
    assert len(datasets) == 3  # b,c,d


def test_insert_dataset_sets_popularity_from_view_counts(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
):
    record = create_record_for_dataset(
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        identifier="dataset-popularity-insert",
    )
    slug = "dataset-popularity-insert"
    interface.db.add(DatasetViewCount(dataset_slug=slug, view_count=9))
    interface.db.commit()

    translated_geojson = {"type": "Point", "coordinates": [-77.0, 38.9]}

    dataset = interface.insert_dataset(
        dataset_payload(
            slug,
            record,
            organization_data,
            source_data_dcatus,
            translated_spatial=translated_geojson,
        )
    )

    assert dataset.popularity == 9
    assert dataset.translated_spatial == translated_geojson


def test_upsert_dataset_updates_popularity_from_view_counts(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
):
    record = create_record_for_dataset(
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        identifier="dataset-popularity-upsert",
    )
    slug = "dataset-popularity-upsert"
    interface.db.add(DatasetViewCount(dataset_slug=slug, view_count=4))
    interface.db.commit()

    base_geojson = {"type": "Point", "coordinates": [-80.0, 25.0]}
    base_payload = dataset_payload(
        slug,
        record,
        organization_data,
        source_data_dcatus,
        translated_spatial=base_geojson,
    )
    interface.insert_dataset(base_payload)

    # simulate analytics refresh increasing popularity
    view_count = (
        interface.db.query(DatasetViewCount).filter_by(dataset_slug=slug).first()
    )
    view_count.view_count = 12
    interface.db.commit()

    updated_geojson = {"type": "Point", "coordinates": [-81.0, 26.0]}
    updated_payload = {
        **base_payload,
        "dcat": {"title": f"{slug}-updated"},
        "last_harvested_date": datetime.now(timezone.utc),
        "translated_spatial": updated_geojson,
    }
    dataset = interface.upsert_dataset(updated_payload)

    assert dataset.popularity == 12
    assert dataset.translated_spatial == updated_geojson


def test_count_missing_outdated_datasets(
    interface,
    organization_data,
    source_data_dcatus,
    job_data_dcatus,
):
    interface.add_organization(organization_data)
    interface.add_harvest_source(source_data_dcatus)
    job_data_dcatus["harvest_source_id"] = source_data_dcatus["id"]
    interface.add_harvest_job(job_data_dcatus)

    # missing dataset harvest record
    interface.add_harvest_record(
        {
            "identifier": "test",
            "harvest_job_id": job_data_dcatus["id"],
            "harvest_source_id": source_data_dcatus["id"],
            "status": "success",
            "action": "create",
            "source_raw": "{}",
        }
    )

    # dataset outdated harvest records
    outdated_record = interface.add_harvest_record(
        {
            "identifier": "test-other",
            "harvest_job_id": job_data_dcatus["id"],
            "harvest_source_id": source_data_dcatus["id"],
            "status": "success",
            "action": "create",
            "source_raw": "{}",
            "date_created": datetime(2000, 3, 1, 0, 0, 0, 1000),
        }
    )

    update_payload = {
        "slug": "test-other",
        "dcat": {"title": "Updated via harvest"},
        "organization_id": organization_data["id"],
        "harvest_source_id": source_data_dcatus["id"],
        "harvest_record_id": outdated_record.id,
        "last_harvested_date": datetime(2000, 3, 1, 0, 0, 0, 1000),
    }

    interface.upsert_dataset(update_payload)

    interface.add_harvest_record(
        {
            "identifier": "test-other",
            "harvest_job_id": job_data_dcatus["id"],
            "harvest_source_id": source_data_dcatus["id"],
            "status": "success",
            "action": "update",
            "source_raw": "{}",
        }
    )

    # 1 missing and 1 outdated dataset
    count, sample_records = interface.get_missing_or_outdated_dataset_count_and_sample()
    assert count == 2 & len(sample_records) == 2
