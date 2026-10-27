"""drop redundant harvest_record_current_issue.harvest_record_id

Revision ID: effa2ad9f73c
Revises: af98cfc82f70
Create Date: 2026-10-23 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "effa2ad9f73c"
down_revision = "af98cfc82f70"
branch_labels = None
depends_on = None


def upgrade():
    # harvest_record_id here always equals the harvest_record_error row's
    # own harvest_record_id (same id, since harvest_record_current_issue.id
    # FKs to it) -- read it through that join instead of duplicating it.
    op.drop_index(
        "ix_harvest_record_current_issue_harvest_record_id",
        table_name="harvest_record_current_issue",
    )
    op.drop_column("harvest_record_current_issue", "harvest_record_id")


def downgrade():
    op.add_column(
        "harvest_record_current_issue",
        sa.Column("harvest_record_id", sa.String(length=36), nullable=True),
    )
    op.create_foreign_key(
        "harvest_record_current_issue_harvest_record_id_fkey",
        "harvest_record_current_issue",
        "harvest_record",
        ["harvest_record_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_harvest_record_current_issue_harvest_record_id",
        "harvest_record_current_issue",
        ["harvest_record_id"],
    )
