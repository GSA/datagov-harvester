"""Make organization.slug non-nullable

Revision ID: f2b1f0f2a9f7
Revises: c6db3bc6a5f5
Create Date: 2026-03-04 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f2b1f0f2a9f7"
down_revision = "c6db3bc6a5f5"
branch_labels = None
depends_on = None


def upgrade():
    # Backfill missing slugs so the NOT NULL alteration succeeds.
    op.execute(sa.text("""
            UPDATE organization
            SET slug = CONCAT('org-', id)
            WHERE slug IS NULL
            """))

    with op.batch_alter_table("organization", schema=None) as batch_op:
        batch_op.alter_column(
            "slug",
            existing_type=sa.String(length=100),
            nullable=False,
        )


def downgrade():
    with op.batch_alter_table("organization", schema=None) as batch_op:
        batch_op.alter_column(
            "slug",
            existing_type=sa.String(length=100),
            nullable=True,
        )
