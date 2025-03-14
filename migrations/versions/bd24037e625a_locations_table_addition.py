"""Locations Table Addition

Revision ID: bd24037e625a
Revises: 63a1161d69b7
Create Date: 2025-03-07 22:50:36.784734

"""

import sqlalchemy as sa
from alembic import op
from geoalchemy2 import Geometry

# revision identifiers, used by Alembic.
revision = "bd24037e625a"
down_revision = "63a1161d69b7"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION postgis;")
    op.create_table(
        "locations",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String),
        sa.Column("type", sa.String),
        sa.Column("display_name", sa.String),
        sa.Column("the_geom", Geometry(geometry_type="MULTIPOLYGON")),
        sa.Column("type_order", sa.String),
    )


def downgrade():
    op.drop_table("locations")
    op.execute("DROP EXTENSION postgis_topology;")
    op.execute("DROP EXTENSION postgis_tiger_geocoder;")
    op.execute("DROP EXTENSION postgis;")
