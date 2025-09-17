"""remove 'csdgm' from schema_type

Revision ID: a6aa1afd27b7
Revises: 1800d355e5b9
Create Date: 2025-09-12 17:06:56.439448

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a6aa1afd27b7'
down_revision = '1800d355e5b9'
branch_labels = None
depends_on = None

# remove "csdgm" from schema_type
old_options = (
    "iso19115_1",
    "iso19115_2",
    "csdgm",
    "dcatus1.1: federal",
    "dcatus1.1: non-federal",
)
new_options = (
    "iso19115_1",
    "iso19115_2",
    "dcatus1.1: federal",
    "dcatus1.1: non-federal",
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

    # Alter column back
    op.execute(
        "ALTER TABLE harvest_source ALTER COLUMN schema_type TYPE schema_type_old "
        "USING schema_type::text::schema_type_old"
    )

    # Drop the present enum
    new_enum.name = "schema_type"
    new_enum.drop(op.get_bind(), checkfirst=False)

    # Rename old enum to current enum
    op.execute("ALTER TYPE schema_type_old RENAME TO schema_type")
