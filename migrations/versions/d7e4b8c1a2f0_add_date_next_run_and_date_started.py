"""add harvest_source.date_next_run and harvest_job.date_started

Revision ID: d7e4b8c1a2f0
Revises: c3e5a7b9d1f3
Create Date: 2026-08-19 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d7e4b8c1a2f0"
down_revision = "c3e5a7b9d1f3"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "harvest_source",
        sa.Column("date_next_run", sa.DateTime(), nullable=True),
    )
    op.create_index(
        op.f("ix_harvest_source_date_next_run"),
        "harvest_source",
        ["date_next_run"],
        unique=False,
    )
    op.add_column(
        "harvest_job",
        sa.Column("date_started", sa.DateTime(), nullable=True),
    )

    # Old in-progress/complete/error rows used date_created as the start time.
    op.execute(
        """
        UPDATE harvest_job
        SET date_started = date_created
        WHERE status IN ('in_progress', 'complete', 'error')
        """
    )

    # Future status=new rows were calendar placeholders. Move that time onto
    # the source, then delete the placeholder jobs.
    op.execute(
        """
        UPDATE harvest_source hs
        SET date_next_run = sub.date_created
        FROM (
            SELECT DISTINCT ON (harvest_source_id)
                harvest_source_id,
                date_created
            FROM harvest_job
            WHERE status = 'new' AND date_created > NOW()
            ORDER BY harvest_source_id, date_created DESC
        ) sub
        WHERE hs.id = sub.harvest_source_id
          AND hs.frequency <> 'manual'
        """
    )
    op.execute(
        """
        DELETE FROM harvest_job
        WHERE status = 'new' AND date_created > NOW()
        """
    )

    # Remaining non-manual sources get a next run in the future so the first
    # scheduler pass after deploy does not enqueue every source.
    op.execute(
        """
        UPDATE harvest_source
        SET date_next_run = NOW() + (
            CASE frequency
                WHEN 'daily' THEN INTERVAL '1 day'
                WHEN 'weekly' THEN INTERVAL '7 days'
                WHEN 'biweekly' THEN INTERVAL '14 days'
                WHEN 'monthly' THEN INTERVAL '30 days'
            END
        )
        WHERE frequency <> 'manual'
          AND date_next_run IS NULL
        """
    )


def downgrade():
    op.drop_column("harvest_job", "date_started")
    op.drop_index(
        op.f("ix_harvest_source_date_next_run"),
        table_name="harvest_source",
    )
    op.drop_column("harvest_source", "date_next_run")
