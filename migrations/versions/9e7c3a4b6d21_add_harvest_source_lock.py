"""Add a persistent harvest source lock.

Revision ID: 9e7c3a4b6d21
Revises: 25d350b0b942
"""

import sqlalchemy as sa
from alembic import op

revision = "9e7c3a4b6d21"
down_revision = "25d350b0b942"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "harvest_source",
        sa.Column("is_locked", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade():
    op.drop_column("harvest_source", "is_locked")
