"""Replace harvest record latest index with success partial index.

Revision ID: 8c1f0d7e9a4b
Revises: 4bc26ce5f9f7
Create Date: 2026-06-24 16:05:00.000000

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "8c1f0d7e9a4b"
down_revision = "4bc26ce5f9f7"
branch_labels = None
depends_on = None


def upgrade():
    with op.get_context().autocommit_block():
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_hr_source_status_identifier_created_desc
            """)
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_harvest_record_source_identifier_created_success
            ON harvest_record (harvest_source_id, identifier, date_created DESC)
            INCLUDE (action)
            WHERE status = 'success'
            """)


def downgrade():
    with op.get_context().autocommit_block():
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_harvest_record_source_identifier_created_success
            """)
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_hr_source_status_identifier_created_desc
            ON harvest_record (
                harvest_source_id,
                status,
                identifier,
                date_created DESC
            )
            """)
