"""add translated spatial column to dataset

Revision ID: a4cb8572c296
Revises: e75e287e2251
Create Date: 2025-03-05 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "a4cb8572c296"
down_revision = "e75e287e2251"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "dataset",
        sa.Column(
            "translated_spatial",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )


def downgrade():
    op.drop_column("dataset", "translated_spatial")
