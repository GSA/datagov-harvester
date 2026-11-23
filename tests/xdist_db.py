import os

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL, make_url

_server_url: URL | None = None
_worker_db: str | None = None


def use_worker_database() -> None:
    """Point this pytest-xdist worker at its own database.

    Must run before the app or harvester modules are imported: they build
    engines from DATABASE_URI at import time.
    """
    global _server_url, _worker_db

    worker = os.getenv("PYTEST_XDIST_WORKER")
    if not worker:
        return

    # Concurrent runs reuse worker ids (gw0, gw1...), so the run id keeps
    # their databases apart. Trimmed to stay under Postgres' 63-char limit.
    run_id = os.environ["PYTEST_XDIST_TESTRUNUID"][:8]
    url = make_url(os.environ["DATABASE_URI"])
    worker_db = f"{url.database}_{run_id}_{worker}"

    engine = create_engine(url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(text(f'CREATE DATABASE "{worker_db}"'))
    engine.dispose()
    _server_url, _worker_db = url, worker_db

    worker_url = url.set(database=worker_db)
    engine = create_engine(worker_url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
    engine.dispose()

    os.environ["DATABASE_URI"] = worker_url.render_as_string(hide_password=False)


def drop_worker_database() -> None:
    """Drop the database created by use_worker_database().

    Callers dispose their own engines first; FORCE ends any connection that
    is still open so the drop can't hang.
    """
    global _worker_db

    if _worker_db is None:
        return

    engine = create_engine(_server_url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{_worker_db}" WITH (FORCE)'))
    engine.dispose()
    _worker_db = None
