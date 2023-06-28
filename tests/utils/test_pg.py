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


# TODO scope session?
@pytest.fixture
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


def test_update_entry_status(postgres_utility):
    job_id = "job123"
    new_status = "Extracted"
    postgres_utility.connection = MagicMock()
    postgres_utility.update_entry_status(job_id, new_status)
    postgres_utility.connection.cursor().execute.assert_called_once_with(
        "UPDATE job_table SET status = %s WHERE job_id = %s", (new_status, job_id)
    )
    postgres_utility.connection.commit.assert_called_once()


def test_perform_bulk_updates(postgres_utility):
    new_status = "In Progress"
    preconditions = "<preconditions>"
    postgres_utility.connection = MagicMock()
    postgres_utility.perform_bulk_updates(new_status, preconditions)
    postgres_utility.connection.cursor().execute.assert_called_once_with(
        "UPDATE job_table SET status = %s WHERE <preconditions>", (new_status,)
    )
    postgres_utility.connection.commit.assert_called_once()
