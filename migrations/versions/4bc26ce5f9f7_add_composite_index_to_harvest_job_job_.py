"""add composite index to harvest job & job_id index to harvest record

Revision ID: 4bc26ce5f9f7
Revises: f2b1f0f2a9f7
Create Date: 2026-06-16 13:32:29.664001

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "4bc26ce5f9f7"
down_revision = "f2b1f0f2a9f7"
branch_labels = None
depends_on = None


def upgrade():
    with op.get_context().autocommit_block():
        op.create_index(
            "ix_hr_source_status_identifier_created_desc",
            "harvest_record",
            [
                "harvest_source_id",
                "status",
                "identifier",
                sa.text("date_created DESC"),
            ],
            unique=False,
            postgresql_concurrently=True,
        )

        op.create_index(
            "ix_hre_job_id",
            "harvest_record_error",
            ["harvest_job_id"],
            unique=False,
            postgresql_concurrently=True,
        )


def downgrade():
    with op.get_context().autocommit_block():
        op.drop_index(
            "ix_hr_source_status_identifier_created_desc",
            table_name="harvest_record",
            postgresql_concurrently=True,
        )
        op.drop_index(
            "ix_hre_job_id",
            table_name="harvest_record_error",
            postgresql_concurrently=True,
        )
