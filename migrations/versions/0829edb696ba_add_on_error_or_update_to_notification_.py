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
down_revision = "1800d355e5b9"
branch_labels = None
depends_on = None


def upgrade():
    # Add the new enum value using PostgreSQL's ALTER TYPE
    op.execute("ALTER TYPE notification_frequency ADD VALUE 'on_error_or_update'")


def downgrade():
    op.execute(
        """
        UPDATE harvest_source 
        SET notification_frequency = 'on_error' 
        WHERE notification_frequency = 'on_error_or_update'
    """
    )

    op.execute(
        """
        ALTER TABLE harvest_source 
        ALTER COLUMN notification_frequency TYPE VARCHAR USING notification_frequency::VARCHAR
    """
    )

    op.execute("DROP TYPE IF EXISTS notification_frequency")

    notification_frequency_enum = ENUM(
        "on_error", "always", name="notification_frequency"
    )
    notification_frequency_enum.create(op.get_bind())

    op.execute(
        """
        ALTER TABLE harvest_source 
        ALTER COLUMN notification_frequency TYPE notification_frequency 
        USING notification_frequency::notification_frequency
    """
    )
