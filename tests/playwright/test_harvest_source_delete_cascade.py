"""Archiving a "cleared" harvest source must preserve its child rows.

The other archive tests cover a source with no records at all, or the 409
refusal when a source still has live records. This test verifies that
`archive_harvest_source` correctly marks a cleared source as archived while
preserving all harvest_record and harvest_record_error rows in the table.

UI archive schedules a CF/local background task and returns immediately with a
"may take some time" flash; this test waits for the archive to finish in the
app database before asserting the source is marked archived and child rows remain.

The fixture seeds a dataset as well as records. Datasets ensure that the archive
operation correctly preserves all related data: `dataset.harvest_source_id` points
to the archived source, and all job/record history remains accessible.

Rows are committed on their own connection because the app under test is a
separate process; the shared `session` fixture rolls back, so anything seeded
through it would be invisible to the browser.

They also have to land in the *app's* database, which is not the one
`DATABASE_URI` names. `make up` runs two stacks: the app under test with its db
on APP_DATABASE_PORT, and a second db on DATABASE_PORT that host-side pytest
owns and that the autouse `dbapp` fixture drops and recreates per test. Seeding
through DATABASE_URI would write to that second db, so the browser would 404 on
the source and never render an Archive button.
"""

import os
import re
import time
import uuid

import pytest
from playwright.sync_api import expect
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

RECORD_COUNT = 25
DELETE_WAIT_SECONDS = 60

# Host port of the db belonging to the app under test. Kept in step with the
# `DATABASE_PORT=5433` override in the Makefile's `up` target.
APP_DATABASE_PORT = int(os.getenv("APP_DATABASE_PORT", "5433"))


def _wait_for_source_gone(engine, source_id, timeout_seconds=DELETE_WAIT_SECONDS):
    """Poll until the background archive task removes the source row (legacy)."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if _child_row_counts(engine, source_id)["sources"] == 0:
            return
        time.sleep(0.25)
    raise AssertionError(
        f"Harvest source {source_id} was not archived within {timeout_seconds}s"
    )


def _wait_for_source_archived(engine, source_id, timeout_seconds=DELETE_WAIT_SECONDS):
    """Poll until the background archive task marks the source as archived."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT is_archived FROM harvest_source WHERE id = :source_id"),
                {"source_id": source_id},
            )
            row = result.fetchone()
            if row and row[0] is True:
                return
        time.sleep(0.25)
    raise AssertionError(
        f"Harvest source {source_id} was not archived within {timeout_seconds}s"
    )


@pytest.fixture()
def engine():
    """Engine on the app-under-test's db, not the host pytest db.

    Credentials, host and database name are reused from DATABASE_URI; only the
    port differs between the two stacks.
    """
    database_uri = os.getenv("DATABASE_URI")
    if not database_uri:
        pytest.skip("DATABASE_URI is required to seed cascade fixtures")

    engine = create_engine(make_url(database_uri).set(port=APP_DATABASE_PORT))
    yield engine
    engine.dispose()


@pytest.fixture()
def cleared_source(engine):
    """A source whose records were all cleared, with errors and a dataset.

    Every record's latest action is "delete", so the UI's record count reports
    zero and the Archive button is allowed through.
    """
    org_id = str(uuid.uuid4())
    source_id = str(uuid.uuid4())
    job_id = str(uuid.uuid4())
    suffix = source_id[:8]
    source_name = f"Cascade Source {suffix}"
    record_ids: list[str] = []

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO organization (id, name, slug) VALUES (:id, :name, :slug)"
            ),
            {
                "id": org_id,
                "name": f"Cascade Org {suffix}",
                "slug": f"cascade-{suffix}",
            },
        )
        conn.execute(
            text("""
                INSERT INTO harvest_source (
                    id, name, notification_emails, organization_id, frequency,
                    notification_frequency, url, schema_type, source_type
                ) VALUES (
                    :id, :name, '{email@example.com}', :org_id, 'daily',
                    'always', :url, 'dcatus1.1: federal', 'document'
                )
            """),
            {
                "id": source_id,
                "name": source_name,
                "org_id": org_id,
                "url": f"http://localhost:80/dcatus/cascade-{suffix}.json",
            },
        )
        conn.execute(
            text(
                "INSERT INTO harvest_job (id, harvest_source_id, status) "
                "VALUES (:id, :source_id, 'complete')"
            ),
            {"id": job_id, "source_id": source_id},
        )

        for index in range(RECORD_COUNT):
            record_id = str(uuid.uuid4())
            record_ids.append(record_id)
            conn.execute(
                text("""
                    INSERT INTO harvest_record (
                        id, harvest_job_id, harvest_source_id, identifier,
                        source_raw, status, action, date_created
                    ) VALUES (
                        :id, :job_id, :source_id, :identifier,
                        :source_raw, 'success', 'delete', now()
                    )
                """),
                {
                    "id": record_id,
                    "job_id": job_id,
                    "source_id": source_id,
                    "identifier": f"cascade-identifier-{index}",
                    "source_raw": '{"title": "cleared"}',
                },
            )
            conn.execute(
                text("""
                    INSERT INTO harvest_record_error (
                        id, harvest_record_id, harvest_job_id, message, type
                    ) VALUES (
                        :id, :record_id, :job_id, 'record is invalid',
                        'ValidationException'
                    )
                """),
                {
                    "id": str(uuid.uuid4()),
                    "record_id": record_id,
                    "job_id": job_id,
                },
            )

        # dataset.harvest_source_id / .harvest_record_id are NOT NULL, so this
        # row is what catches a missing passive_deletes on the backref.
        conn.execute(
            text("""
                INSERT INTO dataset (
                    id, slug, dcat, organization_id, harvest_source_id,
                    harvest_record_id, popularity
                ) VALUES (
                    :id, :slug, '{}', :org_id, :source_id, :record_id, 0
                )
            """),
            {
                "id": str(uuid.uuid4()),
                "slug": f"cascade-dataset-{suffix}",
                "org_id": org_id,
                "source_id": source_id,
                "record_id": record_ids[0],
            },
        )

    yield {"source_id": source_id, "org_id": org_id, "name": source_name}

    # The test archives the source itself, but we still clean up all test data.
    # Tear down child-first for explicit cleanup of the test fixture.
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM dataset WHERE harvest_source_id = :id"),
            {"id": source_id},
        )
        conn.execute(
            text("""
                DELETE FROM harvest_record_error
                WHERE harvest_record_id IN (
                    SELECT id FROM harvest_record WHERE harvest_source_id = :id
                )
            """),
            {"id": source_id},
        )
        conn.execute(
            text("DELETE FROM harvest_record_error WHERE harvest_job_id = :id"),
            {"id": job_id},
        )
        conn.execute(
            text("DELETE FROM harvest_record WHERE harvest_source_id = :id"),
            {"id": source_id},
        )
        conn.execute(
            text("DELETE FROM harvest_job_error WHERE harvest_job_id = :id"),
            {"id": job_id},
        )
        conn.execute(
            text("DELETE FROM harvest_job WHERE harvest_source_id = :id"),
            {"id": source_id},
        )
        conn.execute(
            text("DELETE FROM harvest_source WHERE id = :id"), {"id": source_id}
        )
        conn.execute(text("DELETE FROM organization WHERE id = :id"), {"id": org_id})


def _child_row_counts(engine, source_id):
    with engine.connect() as conn:
        return {
            "sources": conn.execute(
                text("SELECT count(*) FROM harvest_source WHERE id = :id"),
                {"id": source_id},
            ).scalar(),
            "jobs": conn.execute(
                text("SELECT count(*) FROM harvest_job WHERE harvest_source_id = :id"),
                {"id": source_id},
            ).scalar(),
            "records": conn.execute(
                text(
                    "SELECT count(*) FROM harvest_record WHERE harvest_source_id = :id"
                ),
                {"id": source_id},
            ).scalar(),
            "record_errors": conn.execute(
                text("""
                    SELECT count(*)
                    FROM harvest_record_error error
                    JOIN harvest_record record
                        ON error.harvest_record_id = record.id
                    WHERE record.harvest_source_id = :id
                """),
                {"id": source_id},
            ).scalar(),
            "datasets": conn.execute(
                text("SELECT count(*) FROM dataset WHERE harvest_source_id = :id"),
                {"id": source_id},
            ).scalar(),
        }


class TestHarvestSourceArchive:
    def test_archive_cleared_source_preserves_children(
        self, authed_page, engine, cleared_source
    ):
        source_id = cleared_source["source_id"]

        before = _child_row_counts(engine, source_id)
        assert before == {
            "sources": 1,
            "jobs": 1,
            "records": RECORD_COUNT,
            "record_errors": RECORD_COUNT,
            "datasets": 1,
        }

        authed_page.goto(f"/harvest_source/{source_id}")

        # Assert positively that the page found the seeded source before reaching
        # for the Archive button. If the fixture ever seeds a db the app isn't
        # reading, the button is simply absent and a bare click would fail as an
        # opaque locator timeout rather than naming the real problem.
        expect(authed_page.locator("h1")).to_have_text(cleared_source["name"])

        authed_page.once("dialog", lambda dialog: dialog.accept())
        authed_page.get_by_role("button", name=re.compile(r"^Archive\b")).click()

        expect(authed_page.locator(".usa-alert--success")).to_contain_text(
            ["Archived harvest source with ID"]
        )

        _wait_for_source_archived(engine, source_id)

        # After archiving, source is marked archived but related records remain
        assert _child_row_counts(engine, source_id) == {
            "sources": 1,
            "jobs": 1,
            "records": RECORD_COUNT,
            "record_errors": RECORD_COUNT,
            "datasets": 1,
        }
