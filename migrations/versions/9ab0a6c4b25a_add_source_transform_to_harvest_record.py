"""Add source_transform to harvest_record

Revision ID: 9ab0a6c4b25a
Revises: 416319f5e5bb
Create Date: 2025-09-22 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "9ab0a6c4b25a"
down_revision = "416319f5e5bb"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("harvest_record", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "source_transform",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=True,
            )
        )


def downgrade():
    with op.batch_alter_table("harvest_record", schema=None) as batch_op:
        batch_op.drop_column("source_transform")
