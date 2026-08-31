"""add code.json schema type

Revision ID: 9cfe5a1d20f6
Revises: d4f6a8c0e2b3
Create Date: 2026-08-31 09:26:48.380659

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9cfe5a1d20f6"
down_revision = "d4f6a8c0e2b3"
branch_labels = None
depends_on = None


def upgrade():
    # Add 'code.json' value to schema_type enum
    op.execute("ALTER TYPE schema_type ADD VALUE IF NOT EXISTS 'code.json'")


def downgrade():
    # Enum values cannot be removed in PostgreSQL without recreating the type
    # This would require dropping and recreating the enum, which is complex
    # and risks data loss. For safety, downgrade is not implemented.
    pass
