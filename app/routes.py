"""Backward-compatible re-exports; route registration lives in app.blueprints."""

from app.blueprints import register_routes
from app.blueprints.deps import (
    UnsafeTemplateEnvError,
    db,
    load_manager,
    render_block,
)
from app.blueprints.main.auth import create_client_assertion

__all__ = [
    "UnsafeTemplateEnvError",
    "create_client_assertion",
    "db",
    "load_manager",
    "register_routes",
    "render_block",
]
