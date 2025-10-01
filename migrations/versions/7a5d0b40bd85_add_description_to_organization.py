"""Add description column to organization

Revision ID: 7a5d0b40bd85
Revises: 1800d355e5b9
Create Date: 2025-10-01 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "7a5d0b40bd85"
down_revision = "1800d355e5b9"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("organization", schema=None) as batch_op:
        batch_op.add_column(sa.Column("description", sa.Text(), nullable=True))


def downgrade():
    with op.batch_alter_table("organization", schema=None) as batch_op:
        batch_op.drop_column("description")

