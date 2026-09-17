"""Merge send_report_email and code_json branches

Revision ID: a612f5049ccc
Revises: 5b9b5ccf2e34, 9cfe5a1d20f6
Create Date: 2026-09-17 14:44:22.626527

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a612f5049ccc"
down_revision = ("5b9b5ccf2e34", "9cfe5a1d20f6")
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
