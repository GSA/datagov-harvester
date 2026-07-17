from sqlalchemy import false, func

from .models import db


class HarvestTaskControl(db.Model):
    """Singleton control row for starting new harvest tasks."""

    __tablename__ = "harvest_task_control"

    scheduling_paused = db.Column(
        db.Boolean,
        nullable=False,
        default=False,
        server_default=false(),
    )
    updated_at = db.Column(
        db.DateTime,
        nullable=False,
        default=func.statement_timestamp(),
        server_default=func.statement_timestamp(),
        onupdate=func.statement_timestamp(),
    )
