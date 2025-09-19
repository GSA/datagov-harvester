"""Add constraint for waf-collections

Revision ID: 5416f9f5f06d
Revises: d805537d4cf2
Create Date: 2025-09-15 18:29:47.846333

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5416f9f5f06d"
down_revision = "3b56c7e46974"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("harvest_source", schema=None) as batch_op:
        batch_op.create_check_constraint(
            "wafcollectionparenturl",
            "(collection_parent_url IS NULL AND source_type <> 'waf-collection') OR (collection_parent_url IS NOT NULL AND source_type = 'waf-collection')",
        )


def downgrade():
    with op.batch_alter_table("harvest_source", schema=None) as batch_op:
        batch_op.drop_constraint("wafcollectionparenturl")
