"""Add records_warned to harvest_job.

datagov-data-access 1.1.0 added HarvestJob.records_warned, but the harvester
never got a migration for it. Any query that selects HarvestJob fails with
"column harvest_job.records_warned does not exist" as soon as the pin moves
past 1.0.0, which takes down every harvest source page.

server_default="0" so existing rows get a value rather than NULL, matching how
the model treats it (default=0) and how the sibling record counters behave.

Revision ID: f4a2b8c15d93
Revises: b3f7a91c24de
Create Date: 2026-07-30 13:15:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f4a2b8c15d93"
down_revision = "b3f7a91c24de"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("harvest_job", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "records_warned",
                sa.Integer(),
                server_default="0",
                nullable=True,
            )
        )


def downgrade():
    with op.batch_alter_table("harvest_job", schema=None) as batch_op:
        batch_op.drop_column("records_warned")
