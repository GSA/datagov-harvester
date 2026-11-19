import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url


def use_worker_database() -> None:
    """Point this pytest-xdist worker at its own database.

    Must run before the app or harvester modules are imported: they build
    engines from DATABASE_URI at import time.
    """
    worker = os.getenv("PYTEST_XDIST_WORKER")
    if not worker:
        return

    url = make_url(os.environ["DATABASE_URI"])
    worker_db = f"{url.database}_{worker}"

    engine = create_engine(url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :name"),
            {"name": worker_db},
        ).scalar()
        if not exists:
            conn.execute(text(f'CREATE DATABASE "{worker_db}"'))
    engine.dispose()

    worker_url = url.set(database=worker_db)
    engine = create_engine(worker_url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
    engine.dispose()

    os.environ["DATABASE_URI"] = worker_url.render_as_string(hide_password=False)
