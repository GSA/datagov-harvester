"""add dataset_pending to record_status enum

Revision ID: c7fec7091cf5
Revises: fdc22acc3f7a
Create Date: 2026-07-01 09:52:08.650815

"""

from alembic import op
from alembic_postgresql_enum import TableReference

# revision identifiers, used by Alembic.
revision = "c7fec7091cf5"
down_revision = "fdc22acc3f7a"
branch_labels = None
depends_on = None


def upgrade():
    op.sync_enum_values(
        enum_schema="public",
        enum_name="record_status",
        new_values=["error", "success", "dataset_pending"],
        affected_columns=[
            TableReference(
                table_schema="public",
                table_name="harvest_record",
                column_name="status",
            )
        ],
        enum_values_to_rename=[],
    )


def downgrade():
    op.sync_enum_values(
        enum_schema="public",
        enum_name="record_status",
        new_values=["error", "success"],
        affected_columns=[
            TableReference(
                table_schema="public",
                table_name="harvest_record",
                column_name="status",
            )
        ],
        enum_values_to_rename=[],
    )
