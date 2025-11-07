"""Add dataset and view count tables

Revision ID: c69d6790efff
Revises: 7a5d0b40bd85
Create Date: 2025-11-06 16:22:25.796260

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "c69d6790efff"
down_revision = "7a5d0b40bd85"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "dataset_view_count",
        sa.Column("dataset_slug", sa.String(length=100), nullable=False),
        sa.Column("view_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_dataset_view_count_dataset_slug",
        "dataset_view_count",
        ["dataset_slug"],
        unique=True,
    )

    op.create_table(
        "resource_view_count",
        sa.Column("resource_url", sa.String(length=100), nullable=False),
        sa.Column("view_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_resource_view_count_resource_url",
        "resource_view_count",
        ["resource_url"],
        unique=True,
    )


def downgrade():
    op.drop_index(
        "ix_resource_view_count_resource_url",
        table_name="resource_view_count",
    )
    op.drop_table("resource_view_count")

    op.drop_index(
        "ix_dataset_view_count_dataset_slug",
        table_name="dataset_view_count",
    )
    op.drop_table("dataset_view_count")
