"""replace ix_hre_job_id with a covering (job_id, record_id) index

Revision ID: a7d1f9e23c44
Revises: effa2ad9f73c
Create Date: 2026-11-02 00:00:00.000000

The record-issue view groups harvest_record_error by harvest_record_id
within one harvest_job_id and needs each group's earliest date_created.
ix_hre_job_id can filter by job but still has to heap-fetch every matching
row to read record_id/date_created. Including both in the index lets
Postgres answer the group/count/paginate queries with an index-only scan
instead (see GSA/data.gov#6477 performance follow-up).
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "a7d1f9e23c44"
down_revision = "effa2ad9f73c"
branch_labels = None
depends_on = None


def upgrade():
    with op.get_context().autocommit_block():
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_hre_job_record
            ON harvest_record_error (harvest_job_id, harvest_record_id)
            INCLUDE (date_created)
            """)
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_hre_job_id
            """)


def downgrade():
    with op.get_context().autocommit_block():
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_hre_job_id
            ON harvest_record_error (harvest_job_id)
            """)
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_hre_job_record
            """)
