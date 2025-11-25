"""set dataset popularity default to 0

Revision ID: b4cbb5a8dfef
Revises: a4cb8572c296
Create Date: 2025-03-05 00:30:00.000000

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "b4cbb5a8dfef"
down_revision = "a4cb8572c296"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("UPDATE dataset SET popularity = 0 WHERE popularity IS NULL")
    op.alter_column(
        "dataset",
        "popularity",
        existing_type=sa.Numeric(),
        server_default="0",
    )


def downgrade():
    op.alter_column(
        "dataset",
        "popularity",
        existing_type=sa.Numeric(),
        server_default=None,
    )
