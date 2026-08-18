"""add type to dataset

Revision ID: d4f6a8c0e2b3
Revises: c3e5a7b9d1f3
Create Date: 2026-08-14 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "d4f6a8c0e2b3"
down_revision = "c3e5a7b9d1f3"
branch_labels = None
depends_on = None

# Reuses the "record_type" enum already created for harvest_record.record_type;
# every value that gets a Dataset row (currently "dataset" and "data_series")
# is already present there.
record_type_enum = postgresql.ENUM(
    "dataset",
    "data_service",
    "catalog_record",
    "data_series",
    name="record_type",
    create_type=False,
)


def upgrade():
    op.add_column(
        "dataset",
        sa.Column(
            "type",
            record_type_enum,
            nullable=False,
            server_default="dataset",
        ),
    )
    # CONCURRENTLY so the index build doesn't hold a write lock on `dataset`
    # while harvest jobs are inserting/upserting rows.
    with op.get_context().autocommit_block():
        op.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_dataset_type ON dataset (type)"
        )


def downgrade():
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_dataset_type")
    op.drop_column("dataset", "type")
