"""re-denormalize harvest_record_id/severity/type/date_created onto
harvest_record_current_issue

Revision ID: b2e4c7a19d6f
Revises: a7d1f9e23c44
Create Date: 2026-11-02 00:00:00.000000

The source-page views group/count current issues across an entire source.
harvest_record_error has no index on harvest_source_id (only harvest_job_id,
see a7d1f9e23c44), so without these columns that aggregation must join the
error table, which grows with every job ever run across every source. These
columns were originally on this table and were trimmed as redundant (see
af98cfc82f70 and 37573d7d) once id became a 1:1 FK to harvest_record_error.id
-- restoring them trades that normalization for query performance on a
per-page-view path.
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b2e4c7a19d6f"
down_revision = "a7d1f9e23c44"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "harvest_record_current_issue",
        sa.Column("harvest_record_id", sa.String(), nullable=True),
    )
    op.add_column(
        "harvest_record_current_issue",
        sa.Column(
            "severity",
            sa.Enum("error", "warning", name="error_severity"),
            nullable=False,
            server_default="error",
        ),
    )
    op.add_column(
        "harvest_record_current_issue",
        sa.Column("type", sa.String(), nullable=True),
    )
    op.add_column(
        "harvest_record_current_issue",
        sa.Column("date_created", sa.DateTime(), nullable=True),
    )

    op.execute("""
        UPDATE harvest_record_current_issue
        SET harvest_record_id = harvest_record_error.harvest_record_id,
            severity = harvest_record_error.severity,
            type = harvest_record_error.type,
            date_created = harvest_record_error.date_created
        FROM harvest_record_error
        WHERE harvest_record_error.id = harvest_record_current_issue.id
        """)

    with op.get_context().autocommit_block():
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_hrci_source_record
            ON harvest_record_current_issue (harvest_source_id, harvest_record_id)
            INCLUDE (date_created)
            """)
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_hrci_source_severity_type
            ON harvest_record_current_issue (harvest_source_id, severity, type)
            """)


def downgrade():
    with op.get_context().autocommit_block():
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_hrci_source_severity_type
            """)
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_hrci_source_record
            """)

    op.drop_column("harvest_record_current_issue", "date_created")
    op.drop_column("harvest_record_current_issue", "type")
    op.drop_column("harvest_record_current_issue", "severity")
    op.drop_column("harvest_record_current_issue", "harvest_record_id")
