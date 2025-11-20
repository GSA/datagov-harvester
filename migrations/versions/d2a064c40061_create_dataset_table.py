"""Create dataset table

Revision ID: d2a064c40061
Revises: 1c80316df048
Create Date: 2025-11-21 15:30:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "d2a064c40061"
down_revision = "1c80316df048"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "dataset",
        sa.Column("slug", sa.String(), nullable=False),
        sa.Column("dcat", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("organization_id", sa.String(length=36), nullable=False),
        sa.Column("harvest_source_id", sa.String(length=36), nullable=False),
        sa.Column("harvest_record_id", sa.String(length=36), nullable=False),
        sa.Column("popularity", sa.Numeric(), nullable=True),
        sa.Column("last_harvested_date", sa.DateTime(), nullable=True),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.ForeignKeyConstraint(
            ["organization_id"], ["organization.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["harvest_source_id"], ["harvest_source.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["harvest_record_id"], ["harvest_record.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_dataset_slug"), "dataset", ["slug"], unique=True)
    op.create_index(
        op.f("ix_dataset_organization_id"),
        "dataset",
        ["organization_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_dataset_harvest_source_id"),
        "dataset",
        ["harvest_source_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_dataset_harvest_record_id"),
        "dataset",
        ["harvest_record_id"],
        unique=True,
    )
    op.create_index(
        op.f("ix_dataset_last_harvested_date"),
        "dataset",
        ["last_harvested_date"],
        unique=False,
    )

    with op.batch_alter_table("harvest_record", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_harvest_record_ckan_name"))
        batch_op.drop_column("ckan_name")


def downgrade():
    op.drop_index(op.f("ix_dataset_last_harvested_date"), table_name="dataset")
    op.drop_index(op.f("ix_dataset_harvest_record_id"), table_name="dataset")
    op.drop_index(op.f("ix_dataset_harvest_source_id"), table_name="dataset")
    op.drop_index(op.f("ix_dataset_organization_id"), table_name="dataset")
    op.drop_index(op.f("ix_dataset_slug"), table_name="dataset")
    op.drop_table("dataset")

    with op.batch_alter_table("harvest_record", schema=None) as batch_op:
        batch_op.add_column(sa.Column("ckan_name", sa.String(), nullable=True))
        batch_op.create_index(
            batch_op.f("ix_harvest_record_ckan_name"), ["ckan_name"], unique=False
        )
