"""Add harvest_job_id to HarvestRecordError, make harvest_record_id nullable.

Revision ID: 31663584b0de
Revises: c04b66d4d5b9
Create Date: 2025-02-14 14:07:54.835187

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.orm import Session

from database import models

# revision identifiers, used by Alembic.
revision = "31663584b0de"
down_revision = "c04b66d4d5b9"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    session = Session(bind=bind)

    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("harvest_record_error", schema=None) as batch_op:
        batch_op.alter_column(
            "harvest_record_id", existing_type=sa.VARCHAR(), nullable=True
        )
        batch_op.add_column(
            sa.Column("harvest_job_id", sa.String(length=36), nullable=True)
        )

        batch_op.create_foreign_key(
            "harvest_job_id", "harvest_job", ["harvest_job_id"], ["id"]
        )

    harvest_record_errors = session.query(models.HarvestRecordError).all()
    if harvest_record_errors:
        for harvest_record_error in harvest_record_errors:
            job_id = (
                session.query(
                    models.HarvestRecord.harvest_job_id, models.HarvestRecord.id
                )
                .filter_by(id=harvest_record_error.harvest_record_id)
                .first()[0]
            )
            harvest_record_error.harvest_job_id = job_id
            session.commit()
            session.refresh(harvest_record_error)

    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    with op.batch_alter_table("harvest_record_error", schema=None) as batch_op:
        batch_op.drop_column("harvest_job_id")
        batch_op.alter_column(
            "harvest_record_id", existing_type=sa.VARCHAR(), nullable=False
        )

    # ### end Alembic commands ###
