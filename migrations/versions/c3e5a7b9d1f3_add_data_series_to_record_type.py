"""add 'data_series' to record_type

Revision ID: c3e5a7b9d1f3
Revises: b2d4f6a8c0e2
Create Date: 2026-08-12 00:00:00.000001

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c3e5a7b9d1f3"
down_revision = "b2d4f6a8c0e2"
branch_labels = None
depends_on = None

old_options = ("dataset", "data_service", "catalog_record")
new_options = ("dataset", "data_service", "catalog_record", "data_series")

old_enum = sa.Enum(*old_options, name="record_type")
new_enum = sa.Enum(*new_options, name="record_type_new")


def upgrade():
    new_enum.create(op.get_bind(), checkfirst=False)

    op.execute("ALTER TABLE harvest_record ALTER COLUMN record_type DROP DEFAULT")
    op.execute(
        "ALTER TABLE harvest_record ALTER COLUMN record_type TYPE record_type_new "
        "USING record_type::text::record_type_new"
    )
    op.execute(
        "ALTER TABLE harvest_record ALTER COLUMN record_type "
        "SET DEFAULT 'dataset'::record_type_new"
    )

    old_enum.drop(op.get_bind(), checkfirst=False)
    op.execute("ALTER TYPE record_type_new RENAME TO record_type")


def downgrade():
    old_enum.name = "record_type_old"
    old_enum.create(op.get_bind(), checkfirst=False)

    op.execute("ALTER TABLE harvest_record ALTER COLUMN record_type DROP DEFAULT")
    # will fail if any rows use the data_series record_type
    op.execute(
        "ALTER TABLE harvest_record ALTER COLUMN record_type TYPE record_type_old "
        "USING record_type::text::record_type_old"
    )
    op.execute(
        "ALTER TABLE harvest_record ALTER COLUMN record_type "
        "SET DEFAULT 'dataset'::record_type_old"
    )

    new_enum.name = "record_type"
    new_enum.drop(op.get_bind(), checkfirst=False)

    op.execute("ALTER TYPE record_type_old RENAME TO record_type")
