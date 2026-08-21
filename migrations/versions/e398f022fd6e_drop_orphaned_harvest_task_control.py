"""drop orphaned harvest_task_control table

Revision ID: e398f022fd6e
Revises: c3e5a7b9d1f3
Create Date: 2026-08-21 00:00:00.000001

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e398f022fd6e"
down_revision = "c3e5a7b9d1f3"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("DROP TABLE IF EXISTS harvest_task_control")


def downgrade():
    op.create_table(
        "harvest_task_control",
        sa.Column(
            "scheduling_paused",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.func.statement_timestamp(),
            nullable=False,
        ),
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
