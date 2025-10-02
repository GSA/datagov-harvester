"""Add description and slug columns to organization

Revision ID: 7a5d0b40bd85
Revises: 1800d355e5b9
Create Date: 2025-10-01 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision = "7a5d0b40bd85"
down_revision = "416319f5e5bb"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("organization", schema=None) as batch_op:
        batch_op.add_column(sa.Column("description", sa.Text(), nullable=True))
        batch_op.add_column(sa.Column("slug", sa.String(length=100), nullable=True))
        batch_op.create_unique_constraint("uq_organization_slug", ["slug"])


def downgrade():
    with op.batch_alter_table("organization", schema=None) as batch_op:
        batch_op.drop_constraint("uq_organization_slug", type_="unique")
        batch_op.drop_column("slug")
        batch_op.drop_column("description")

