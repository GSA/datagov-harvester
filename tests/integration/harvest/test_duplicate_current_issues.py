import uuid

from database.models import HarvestRecordCurrentIssue
from harvester.exceptions import DuplicateIdentifierException
from harvester.harvest import HarvestSource

DUPLICATE = DuplicateIdentifierException.__name__


def _duplicate_issues(interface, source_id):
    return (
        interface.db.query(HarvestRecordCurrentIssue)
        .filter_by(harvest_source_id=source_id, type=DUPLICATE)
        .all()
    )


def _run_duplicate_filter(interface, job_data, records):
    job = interface.add_harvest_job({**job_data, "id": str(uuid.uuid4())})
    harvest_source = HarvestSource(job.id)
    harvest_source.external_records = [dict(r) for r in records]
    harvest_source.filter_duplicate_identifiers()
    return harvest_source


class TestDuplicateCurrentIssues:
    def test_repeat_harvests_do_not_accumulate_duplicate_issues(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        duplicated_identifier_records,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        source_id = source_data_dcatus["id"]

        first = _run_duplicate_filter(
            interface, job_data_dcatus, duplicated_identifier_records
        )
        assert len(first.external_records) == 1
        assert len(_duplicate_issues(interface, source_id)) == 2

        second = _run_duplicate_filter(
            interface, job_data_dcatus, duplicated_identifier_records
        )
        issues = _duplicate_issues(interface, source_id)
        assert len(issues) == 2
        second_job_record_ids = {
            r.id
            for r in interface.get_harvest_records_by_job(second.job_id, paginate=False)
        }
        assert {i.harvest_record_id for i in issues} == second_job_record_ids

    def test_resolved_duplicate_clears_outstanding_issue(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        duplicated_identifier_records,
    ):
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        source_id = source_data_dcatus["id"]

        _run_duplicate_filter(interface, job_data_dcatus, duplicated_identifier_records)
        assert len(_duplicate_issues(interface, source_id)) == 2

        _run_duplicate_filter(
            interface, job_data_dcatus, duplicated_identifier_records[:1]
        )
        assert _duplicate_issues(interface, source_id) == []

    def test_record_sync_keeps_duplicate_issue(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        duplicated_identifier_records,
    ):
        """Re-syncing the kept copy of an identifier must not wipe the
        duplicate error recorded for its extra copies."""
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        source_id = source_data_dcatus["id"]

        harvest_source = _run_duplicate_filter(
            interface, job_data_dcatus, duplicated_identifier_records
        )
        kept = interface.add_harvest_record(
            {
                "identifier": duplicated_identifier_records[0]["identifier"],
                "harvest_job_id": harvest_source.job_id,
                "harvest_source_id": source_id,
                "record_type": "dataset",
                "status": "success",
            }
        )
        assert interface.sync_current_record_issues(
            source_id, "dataset", kept.identifier, kept.id, skip_types=(DUPLICATE,)
        )

        assert len(_duplicate_issues(interface, source_id)) == 2

    def test_resync_from_duplicate_placeholder_succeeds(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        duplicated_identifier_records,
    ):
        """The backfill resyncs an identifier from its latest attempt, which
        for an unchanged duplicated identifier is the duplicate placeholder
        record. That must not re-insert the duplicate issue it already has."""
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        source_id = source_data_dcatus["id"]

        _run_duplicate_filter(interface, job_data_dcatus, duplicated_identifier_records)
        placeholder = _duplicate_issues(interface, source_id)[0]

        assert interface.sync_current_record_issues(
            source_id,
            placeholder.record_type,
            placeholder.identifier,
            placeholder.harvest_record_id,
            skip_types=(DUPLICATE,),
        )
        assert len(_duplicate_issues(interface, source_id)) == 2


class TestRecordsDroppedFromSource:
    def test_dropping_a_record_that_never_harvested_clears_its_issues(
        self,
        interface,
        organization_data,
        source_data_dcatus,
        job_data_dcatus,
        record_data_dcatus,
    ):
        """A record that only ever errored is never deleted (only records whose
        last harvest succeeded are), so dropping it from the source must clear
        its outstanding issues some other way."""
        interface.add_organization(organization_data)
        interface.add_harvest_source(source_data_dcatus)
        source_id = source_data_dcatus["id"]
        old_job = interface.add_harvest_job(
            {**job_data_dcatus, "id": str(uuid.uuid4())}
        )
        for identifier in ("dropped", "kept"):
            record = interface.add_harvest_record(
                {
                    **record_data_dcatus[2],
                    "id": str(uuid.uuid4()),
                    "identifier": identifier,
                    "harvest_job_id": old_job.id,
                    "status": "error",
                }
            )
            interface.add_harvest_record_error(
                {
                    "message": "invalid",
                    "type": "ValidationException",
                    "harvest_job_id": old_job.id,
                    "harvest_record_id": record.id,
                    "severity": "error",
                }
            )

        job = interface.add_harvest_job({**job_data_dcatus, "id": str(uuid.uuid4())})
        harvest_source = HarvestSource(job.id)
        harvest_source.external_records = [{"identifier": "kept"}]
        harvest_source.determine_internal_deletions()

        remaining = (
            interface.db.query(HarvestRecordCurrentIssue)
            .filter_by(harvest_source_id=source_id)
            .all()
        )
        assert {issue.identifier for issue in remaining} == {"kept"}
        assert harvest_source.deletions == set()
