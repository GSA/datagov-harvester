"""Create Locations Table

Revision ID: 350eec1fadff
Revises: c8fc36510d0b
Create Date: 2025-03-06 10:27:32.366470

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '350eec1fadff'
down_revision = 'c8fc36510d0b'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "locations",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String),
        sa.Column("type", sa.String),
        sa.Column("display_name", sa.String),
        sa.Column("type_order", sa.String),
        sa.Column("the_geom", sa.Geometry(geometry_type="MULTIPOLYGON")),
    )


def downgrade():
    op.drop_table('locations')
