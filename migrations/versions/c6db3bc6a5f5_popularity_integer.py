"""change dataset popularity to integer

Revision ID: c6db3bc6a5f5
Revises: b4cbb5a8dfef
Create Date: 2025-03-05 01:10:00.000000

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "c6db3bc6a5f5"
down_revision = "b4cbb5a8dfef"
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column(
        "dataset",
        "popularity",
        existing_type=sa.Numeric(),
        type_=sa.Integer(),
        postgresql_using="GREATEST(0, COALESCE(popularity, 0))::integer",
        server_default="0",
    )


def downgrade():
    op.alter_column(
        "dataset",
        "popularity",
        existing_type=sa.Integer(),
        type_=sa.Numeric(),
        postgresql_using="popularity::numeric",
        server_default="0",
    )
