"""Enable PostGIS

Revision ID: c8fc36510d0b
Revises: c04b66d4d5b9
Create Date: 2025-03-06 10:23:51.468502

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c8fc36510d0b"
down_revision = "c04b66d4d5b9"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION postgis;")


def downgrade():
    op.execute("DROP EXTENSION postgis;")
