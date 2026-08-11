"""add severity to harvest_record_error

Revision ID: e2356e60edfb
Revises: 1f6d2c9a8b3e
Create Date: 2026-07-13 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e2356e60edfb"
down_revision = "1f6d2c9a8b3e"
branch_labels = None
depends_on = None

severity_enum = sa.Enum("error", "warning", name="error_severity")


def upgrade():
    bind = op.get_bind()
    severity_enum.create(bind, checkfirst=True)
    op.add_column(
        "harvest_record_error",
        sa.Column(
            "severity",
            severity_enum,
            nullable=False,
            server_default="error",
        ),
    )


def downgrade():
    op.drop_column("harvest_record_error", "severity")
    severity_enum.drop(op.get_bind(), checkfirst=True)
