"""Add harvest task scheduling control.

Revision ID: 2d7a9c4e6b1f
Revises: 1f6d2c9a8b3e
Create Date: 2026-07-16 17:30:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2d7a9c4e6b1f"
down_revision = "1f6d2c9a8b3e"
branch_labels = None
depends_on = None

CONTROL_ROW_ID = "global"


def upgrade():
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
    op.bulk_insert(
        sa.table(
            "harvest_task_control",
            sa.column("id", sa.String(length=36)),
            sa.column("scheduling_paused", sa.Boolean()),
        ),
        [{"id": CONTROL_ROW_ID, "scheduling_paused": False}],
    )


def downgrade():
    op.drop_table("harvest_task_control")
