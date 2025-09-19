"""add on_error_or_update to notification_frequency enum

Revision ID: 0829edb696ba
Revises: 1800d355e5b9
Create Date: 2025-09-16 14:30:26.160149

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ENUM


# revision identifiers, used by Alembic.
revision = "0829edb696ba"
down_revision = "a6aa1afd27b7"
branch_labels = None
depends_on = None


old_options = (
    "on_error",
    "always",
)
new_options = (
    "on_error",
    "always",
    "on_error_or_update",
)

old_enum = sa.Enum(*old_options, name="notification_frequency")
new_enum = sa.Enum(*new_options, name="notification_frequency_new")


def upgrade():
    # Create new enum
    new_enum.create(op.get_bind(), checkfirst=False)

    # Alter column to use new enum
    op.execute(
        "ALTER TABLE harvest_source ALTER COLUMN notification_frequency TYPE notification_frequency_new "
        "USING notification_frequency::text::notification_frequency_new"
    )

    # Drop old enum
    old_enum.drop(op.get_bind(), checkfirst=False)

    # Rename new enum to old name
    op.execute("ALTER TYPE notification_frequency_new RENAME TO notification_frequency")


def downgrade():
    # recreate the old enum
    old_enum.name = "notification_frequency_old"
    old_enum.create(op.get_bind(), checkfirst=False)

    # Alter column back
    op.execute(
        "ALTER TABLE harvest_source ALTER COLUMN notification_frequency TYPE notification_frequency_old "
        "USING notification_frequency::text::notification_frequency_old"
    )

    # Drop the present enum
    new_enum.name = "notification_frequency"
    new_enum.drop(op.get_bind(), checkfirst=False)

    # Rename old enum to current enum
    op.execute("ALTER TYPE notification_frequency_old RENAME TO notification_frequency")
