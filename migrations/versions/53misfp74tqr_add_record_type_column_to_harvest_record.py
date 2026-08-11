"""add record_type column to harvest_record

Revision ID: 53misfp74tqr
Revises: b3f7a91c24de
Create Date: 2026-08-11 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "53misfp74tqr"
down_revision = "b3f7a91c24de"
branch_labels = None
depends_on = None


def upgrade():
    # Create the record_type enum type
    record_type_enum = sa.Enum(
        "dataset",
        "catalog_record",
        "data_service",
        "dataset_series",
        "catalog",
        name="record_type",
    )
    record_type_enum.create(op.get_bind(), checkfirst=True)

    # Add the record_type column with default value 'dataset'
    op.add_column(
        "harvest_record",
        sa.Column(
            "record_type",
            record_type_enum,
            nullable=False,
            server_default="dataset",
        ),
    )

    # Create index on record_type column for efficient filtering
    op.create_index(
        "ix_harvest_record_record_type",
        "harvest_record",
        ["record_type"],
        unique=False,
    )


def downgrade():
    # Drop the index
    op.drop_index("ix_harvest_record_record_type", table_name="harvest_record")

    # Drop the column
    op.drop_column("harvest_record", "record_type")

    # Drop the enum type
    record_type_enum = sa.Enum(
        "dataset",
        "catalog_record",
        "data_service",
        "dataset_series",
        "catalog",
        name="record_type",
    )
    record_type_enum.drop(op.get_bind(), checkfirst=True)
