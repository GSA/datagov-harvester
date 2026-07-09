"""Add indexes for foreign key lookup paths.

Revision ID: 1f6d2c9a8b3e
Revises: 8c1f0d7e9a4b
Create Date: 2026-06-24 17:35:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "1f6d2c9a8b3e"
down_revision = "8c1f0d7e9a4b"
branch_labels = None
depends_on = None


def upgrade():
    with op.get_context().autocommit_block():
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_harvest_record_harvest_job_id
            ON harvest_record (harvest_job_id)
            """)
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_harvest_record_error_harvest_record_id
            ON harvest_record_error (harvest_record_id)
            """)
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_harvest_job_error_harvest_job_id
            ON harvest_job_error (harvest_job_id)
            """)
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_harvest_source_organization_id
            ON harvest_source (organization_id)
            """)


def downgrade():
    with op.get_context().autocommit_block():
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_harvest_source_organization_id
            """)
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_harvest_job_error_harvest_job_id
            """)
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_harvest_record_error_harvest_record_id
            """)
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_harvest_record_harvest_job_id
            """)
