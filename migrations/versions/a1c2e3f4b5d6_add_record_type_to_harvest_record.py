"""add record_type to harvest_record

Revision ID: a1c2e3f4b5d6
Revises: 428e3ffa02ea
Create Date: 2026-08-10 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a1c2e3f4b5d6"
down_revision = "428e3ffa02ea"
branch_labels = None
depends_on = None

record_type_enum = sa.Enum("dataset", "data_service", name="record_type")


def upgrade():
    bind = op.get_bind()
    record_type_enum.create(bind, checkfirst=True)
    op.add_column(
        "harvest_record",
        sa.Column(
            "record_type",
            record_type_enum,
            nullable=False,
            server_default="dataset",
        ),
    )
    op.create_index(
        "ix_harvest_record_record_type",
        "harvest_record",
        ["record_type"],
    )

    with op.get_context().autocommit_block():
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_harvest_record_source_identifier_created_success
            """)
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_harvest_record_source_type_identifier_created_success
            ON harvest_record (harvest_source_id, record_type, identifier, date_created DESC)
            INCLUDE (action)
            WHERE status = 'success'
            """)


def downgrade():
    with op.get_context().autocommit_block():
        op.execute("""
            DROP INDEX CONCURRENTLY IF EXISTS
            ix_harvest_record_source_type_identifier_created_success
            """)
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS
            ix_harvest_record_source_identifier_created_success
            ON harvest_record (harvest_source_id, identifier, date_created DESC)
            INCLUDE (action)
            WHERE status = 'success'
            """)

    op.drop_index("ix_harvest_record_record_type", table_name="harvest_record")
    op.drop_column("harvest_record", "record_type")
    record_type_enum.drop(op.get_bind(), checkfirst=True)
