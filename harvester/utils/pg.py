from os import getenv

import psycopg
from dotenv import load_dotenv

# ruff: noqa: F841

load_dotenv()


class PostgresUtility:
    def __init__(
        self,
        host=getenv("PG_HOST"),
        port=getenv("PG_PORT"),
        user=getenv("PG_USER"),
        password=getenv("PG_PASS"),
        db=getenv("PG_DB"),
        table=getenv("PG_TABLE"),
    ):
        self.conn = psycopg.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=password,
        )
        self.table = table
        self.cur = self.conn.cursor()

    def query(self, query):
        try:
            self.conn.execute(query)
            self.conn.commit()
        except (Exception, psycopg.Error) as error:
            print("Query Error:", error)

    def close(self):
        self.cur.close()
        self.conn.close()

    def store_new_entry(self, job_id):
        # TODO verify query
        self.query(
            f"""
        INSERT INTO {self.table}
            (jobid, status) VALUES ({job_id}, 'new')
        """
        )
        print("New harvest job id {job_id} stored successfully.")

    def update_entry_status(self, job_id, new_status):
        # TODO verify query
        self.query(
            f"""
        UPDATE {self.table}
            SET status = {new_status} WHERE jobid = {job_id}
        """
        )
        print("Entry status updated successfully.")

    # Perform bulk updates on multiple entries
    def perform_bulk_updates(self, new_status, preconditions):
        # TODO verify query
        query = "UPDATE job_table SET status = %s WHERE <preconditions>"
        try:
            cursor = self.conn.cursor()
            # Build the query with the specified preconditions
            # …
            cursor.execute(query, (new_status,))
            self.conn.commit()
            cursor.close()
            print("Bulk updates performed successfully.")
        except (Exception, psycopg.Error) as error:
            print("Error while performing bulk updates:", error)
