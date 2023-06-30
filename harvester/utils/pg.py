import os

import dotenv
import psycopg

# ruff: noqa: F841

dotenv.load_dotenv()


def get_pg_connection():
    # TODO put build connection stuff back in
    pass


class PostgresUtility:
    def __init__(self, conn):
        self.conn = conn
        self.table = os.getenv("POSTGRES_TABLE")
        self.f_id = os.getenv("POSTGRES_FIELD_ID")
        self.f_status = os.getenv("POSTGRES_FIELD_STATUS")

    def query(self, query):
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                self.conn.commit()
        except (Exception, psycopg.Error) as error:
            print("Query Error:", error)

    def close(self):
        self.conn.close()

    def store_new_entry(self, job_id: str):
        """Stores new harvest job. Accepts job_id string"""
        # TODO verify query for final schema
        self.query(
            f"""
        INSERT INTO {self.table} ({self.f_id}, {self.f_status})
            VALUES ('{job_id}', 'new');
        """
        )
        print(f"New harvest job id {job_id} stored successfully.")

    def update_entry_status(self, job_id: str, new_status: str):
        """Updates a harvestjob entry. Accepts job_id and new_status"""
        # TODO verify query for final schema
        self.query(
            f"""
        UPDATE {self.table}
            SET {self.f_status}='{new_status}'
            WHERE {self.f_id}='{job_id}'
        """
        )
        print(f"Job {job_id} status updated successfully to {new_status}.")

    def bulk_update(self, update_list: list):
        """Bulk updates harvestjobs. Accepts list [[job_id, new_status]]."""

        update_table = ""
        for jobid, new_status in update_list:
            update_table += f"('{jobid}', '{new_status}'),"
        update_table = update_table.rstrip(",")

        query = f"""UPDATE {self.table} as orig
            SET {self.f_status}=updates.status
            FROM (values {update_table}) as updates(jobid, status)
            WHERE updates.jobid=orig.{self.f_id}
            """
        self.query(query)
        print(f"Successsfully bulk updated {len(update_list)} harvet jobs.")
