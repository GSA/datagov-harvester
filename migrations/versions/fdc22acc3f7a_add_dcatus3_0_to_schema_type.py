"""add 'dcatus3.0' to schema_type

Revision ID: fdc22acc3f7a
Revises: 4bc26ce5f9f7
Create Date: 2026-06-22 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "fdc22acc3f7a"
down_revision = "4bc26ce5f9f7"
branch_labels = None
depends_on = None

# add the dcatus3.0 schema type
old_options = (
    "iso19115_1",
    "iso19115_2",
    "dcatus1.1: federal",
    "dcatus1.1: non-federal",
)
new_options = (
    "iso19115_1",
    "iso19115_2",
    "dcatus1.1: federal",
    "dcatus1.1: non-federal",
    "dcatus3.0",
)

old_enum = sa.Enum(*old_options, name="schema_type")
new_enum = sa.Enum(*new_options, name="schema_type_new")


def upgrade():
    # Create new enum
    new_enum.create(op.get_bind(), checkfirst=False)

    # Alter column to use new enum
    op.execute(
        "ALTER TABLE harvest_source ALTER COLUMN schema_type TYPE schema_type_new "
        "USING schema_type::text::schema_type_new"
    )

    # Drop old enum
    old_enum.drop(op.get_bind(), checkfirst=False)

    # Rename new enum to old name
    op.execute("ALTER TYPE schema_type_new RENAME TO schema_type")


def downgrade():
    # recreate the old enum
    old_enum.name = "schema_type_old"
    old_enum.create(op.get_bind(), checkfirst=False)

    # Alter column back (will fail if any rows use the dcatus3.0 schema_type)
    op.execute(
        "ALTER TABLE harvest_source ALTER COLUMN schema_type TYPE schema_type_old "
        "USING schema_type::text::schema_type_old"
    )

    # Drop the present enum
    new_enum.name = "schema_type"
    new_enum.drop(op.get_bind(), checkfirst=False)

    # Rename old enum to current enum
    op.execute("ALTER TYPE schema_type_old RENAME TO schema_type")
