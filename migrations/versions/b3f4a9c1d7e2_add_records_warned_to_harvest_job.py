"""add records_warned to harvest_job

Revision ID: b3f4a9c1d7e2
Revises: e2356e60edfb
Create Date: 2026-07-20 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b3f4a9c1d7e2"
down_revision = "e2356e60edfb"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "harvest_job",
        sa.Column("records_warned", sa.Integer(), server_default="0"),
    )


def downgrade():
    op.drop_column("harvest_job", "records_warned")
