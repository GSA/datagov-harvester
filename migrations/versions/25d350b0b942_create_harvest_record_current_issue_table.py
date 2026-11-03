"""create harvest_record_current_issue table

Revision ID: 25d350b0b942
Revises: cd1b89204949
Create Date: 2026-10-15 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "25d350b0b942"
down_revision = "cd1b89204949"
branch_labels = None
depends_on = None

# Reuses the enums already created for harvest_record.record_type and
# harvest_record_error.severity.
record_type_enum = postgresql.ENUM(
    "dataset",
    "data_service",
    "catalog_record",
    "data_series",
    name="record_type",
    create_type=False,
)
severity_enum = postgresql.ENUM(
    "error",
    "warning",
    name="error_severity",
    create_type=False,
)


def upgrade():
    op.create_table(
        "harvest_record_current_issue",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("date_created", sa.DateTime(), nullable=True),
        sa.Column("type", sa.String(), nullable=True),
        sa.Column("message", sa.String(), nullable=True),
        sa.Column("harvest_source_id", sa.String(length=36), nullable=False),
        sa.Column(
            "record_type", record_type_enum, nullable=False, server_default="dataset"
        ),
        sa.Column("identifier", sa.String(), nullable=False),
        sa.Column("harvest_record_id", sa.String(length=36), nullable=True),
        sa.Column("severity", severity_enum, nullable=False, server_default="error"),
        sa.ForeignKeyConstraint(
            ["harvest_source_id"], ["harvest_source.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["harvest_record_id"], ["harvest_record.id"], ondelete="SET NULL"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_harvest_record_current_issue_source_type_identifier",
        "harvest_record_current_issue",
        ["harvest_source_id", "record_type", "identifier"],
    )
    op.create_index(
        "ix_harvest_record_current_issue_harvest_record_id",
        "harvest_record_current_issue",
        ["harvest_record_id"],
    )


def downgrade():
    op.drop_index(
        "ix_harvest_record_current_issue_harvest_record_id",
        table_name="harvest_record_current_issue",
    )
    op.drop_index(
        "ix_harvest_record_current_issue_source_type_identifier",
        table_name="harvest_record_current_issue",
    )
    op.drop_table("harvest_record_current_issue")
