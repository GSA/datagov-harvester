import logging
import os

import dotenv
import psycopg
import pytest

from harvester.utils.pg import PostgresUtility

# TODO is this the logging we want?
logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

dotenv.load_dotenv()


@pytest.fixture(scope="session")
def table_schema():
    class TableSchema:
        def __init__(self) -> None:
            self.table = os.getenv("POSTGRES_TABLE")
            self.jobid_field = "jobid"
            self.schema = f"""
                CREATE TABLE {self.table} (
                    id SERIAL PRIMARY KEY,
                    {self.jobid_field} TEXT,
                    status TEXT NOT NULL
                )
            """

    return TableSchema()


@pytest.fixture
def postgres_conn(table_schema):
    host = os.getenv("POSTGRES_HOST")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    connection_string = f"host={host} dbname={db} user={user} password={password}"
    conn = psycopg.connect(connection_string)
    with conn.cursor() as cur:
        query = f"DROP TABLE IF EXISTS {table_schema.table};"
        query += table_schema.schema
        cur.execute(query)

    yield conn

    conn.close()


def test_store_new_entry(postgres_conn, table_schema):
    test_job_id = "test_store_new_entry"
    pg = PostgresUtility(postgres_conn)
    pg.store_new_entry(test_job_id)

    with postgres_conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT * FROM {table_schema.table} 
            WHERE {table_schema.jobid_field}='{test_job_id}'
            """
        )
        results = cur.fetchall()

    assert len(results) == 1

    # TODO based on current schema, will need to change when schema changes
    col_id, col_jobid, col_status = results[0]
    assert test_job_id in col_jobid


# TODO
# @pytest.mark.parametrize("")
def test_update_entry_status(postgres_conn, table_schema):
    new_status = "extract"
    test_job_id = "test_update_entry"
    pg = PostgresUtility(postgres_conn)
    pg.store_new_entry(test_job_id)
    pg.update_entry_status(test_job_id, new_status)

    with postgres_conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT * FROM {table_schema.table} 
            WHERE {table_schema.jobid_field}='{test_job_id}'
            """
        )
        results = cur.fetchall()

    assert len(results) == 1

    # TODO based on current schema, will need to change when schema changes
    col_id, col_jobid, col_status = results[0]
    assert test_job_id in col_jobid
    assert new_status in col_status


# @pytest.mark.parametrize("")
def test_perform_bulk_updates(postgres_conn, table_schema):
    updates = [["test_bulk_1", "new", "extract"], ["test_bulk_2", "extract", "compare"]]
    bulk_updates = []
    pg = PostgresUtility(postgres_conn)
    for update in updates:
        job_id, status, new_status = update
        pg.store_new_entry(job_id)
        if status != "new":
            pg.update_entry_status(job_id, status)
        bulk_updates.append([job_id, new_status])

    pg.bulk_update(bulk_updates)

    with postgres_conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT * FROM {table_schema.table} 
            """
        )
        results = cur.fetchall()

    assert len(results) == len(bulk_updates)

    pass
