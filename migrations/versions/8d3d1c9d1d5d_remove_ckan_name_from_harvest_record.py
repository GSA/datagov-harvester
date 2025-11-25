"""Remove ckan_name from harvest_record

Revision ID: 8d3d1c9d1d5d
Revises: d2a064c40061
Create Date: 2025-12-05 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "8d3d1c9d1d5d"
down_revision = "d2a064c40061"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("harvest_record", schema=None) as batch_op:
        batch_op.drop_index(batch_op.f("ix_harvest_record_ckan_name"))
        batch_op.drop_column("ckan_name")


def downgrade():
    with op.batch_alter_table("harvest_record", schema=None) as batch_op:
        batch_op.add_column(sa.Column("ckan_name", sa.String(), nullable=True))
        batch_op.create_index(
            batch_op.f("ix_harvest_record_ckan_name"), ["ckan_name"], unique=False
        )

