"""Add dcatus_catalog to harvest_job

Revision ID: f8d4c5ec0e5b
Revises: b3f7a91c24de
Create Date: 2026-08-05 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "f8d4c5ec0e5b"
down_revision = "b3f7a91c24de"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "harvest_job",
        sa.Column("dcatus_catalog", postgresql.JSONB(), nullable=True),
    )


def downgrade():
    op.drop_column("harvest_job", "dcatus_catalog")
