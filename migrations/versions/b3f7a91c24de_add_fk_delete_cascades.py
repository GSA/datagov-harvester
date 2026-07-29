"""Push harvest source/job/record delete cascades into the database.

Deleting a harvest source used to be done row-by-row by SQLAlchemy, which loaded
every child harvest_record (including source_raw) into the web worker and OOM-killed
it on large sources. With ON DELETE CASCADE in place the ORM can set
passive_deletes=True and let Postgres do the whole cascade in one statement.

harvest_record_error.harvest_record_id becomes SET NULL rather than CASCADE on
purpose: record errors outlive the record they describe. They are still cleaned up
when the owning job or source goes away, via harvest_job_id's CASCADE.

Each constraint is re-added as NOT VALID and validated separately. Adding a normal
FK takes ACCESS EXCLUSIVE while it scans the whole child table; NOT VALID skips that
scan, and the later VALIDATE CONSTRAINT only needs SHARE UPDATE EXCLUSIVE. That
matters because harvest_record is large in prod.

Revision ID: b3f7a91c24de
Revises: e2356e60edfb
Create Date: 2026-07-29 16:05:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "b3f7a91c24de"
down_revision = "e2356e60edfb"
branch_labels = None
depends_on = None


# (constraint, table, column, referenced table, referenced column)
FOREIGN_KEYS = [
    (
        "harvest_source_organization_id_fkey",
        "harvest_source",
        "organization_id",
        "organization",
        "id",
    ),
    (
        "harvest_job_harvest_source_id_fkey",
        "harvest_job",
        "harvest_source_id",
        "harvest_source",
        "id",
    ),
    (
        "harvest_record_harvest_job_id_fkey",
        "harvest_record",
        "harvest_job_id",
        "harvest_job",
        "id",
    ),
    (
        "harvest_record_harvest_source_id_fkey",
        "harvest_record",
        "harvest_source_id",
        "harvest_source",
        "id",
    ),
    (
        "harvest_job_error_harvest_job_id_fkey",
        "harvest_job_error",
        "harvest_job_id",
        "harvest_job",
        "id",
    ),
    (
        "harvest_record_error_harvest_job_id_fkey",
        "harvest_record_error",
        "harvest_job_id",
        "harvest_job",
        "id",
    ),
]

# Deleting a harvest_record must not delete its errors, only orphan them.
SET_NULL_KEYS = [
    (
        "harvest_record_error_harvest_record_id_fkey",
        "harvest_record_error",
        "harvest_record_id",
        "harvest_record",
        "id",
    ),
]


def _replace_fk(constraint, table, column, ref_table, ref_column, on_delete):
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}")
    op.execute(
        f"ALTER TABLE {table} ADD CONSTRAINT {constraint} "
        f"FOREIGN KEY ({column}) REFERENCES {ref_table} ({ref_column}) "
        f"ON DELETE {on_delete} NOT VALID"
    )
    op.execute(f"ALTER TABLE {table} VALIDATE CONSTRAINT {constraint}")


def upgrade():
    # The cascade from harvest_source -> harvest_job traverses this FK, and it was
    # the one lookup path without an index (see 1f6d2c9a8b3e for the others).
    with op.get_context().autocommit_block():
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_harvest_job_harvest_source_id
            ON harvest_job (harvest_source_id)
            """)

    for constraint, table, column, ref_table, ref_column in FOREIGN_KEYS:
        _replace_fk(constraint, table, column, ref_table, ref_column, "CASCADE")

    for constraint, table, column, ref_table, ref_column in SET_NULL_KEYS:
        _replace_fk(constraint, table, column, ref_table, ref_column, "SET NULL")


def downgrade():
    for constraint, table, column, ref_table, ref_column in (
        FOREIGN_KEYS + SET_NULL_KEYS
    ):
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}")
        op.execute(
            f"ALTER TABLE {table} ADD CONSTRAINT {constraint} "
            f"FOREIGN KEY ({column}) REFERENCES {ref_table} ({ref_column}) NOT VALID"
        )
        op.execute(f"ALTER TABLE {table} VALIDATE CONSTRAINT {constraint}")

    with op.get_context().autocommit_block():
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_harvest_job_harvest_source_id
            """)
