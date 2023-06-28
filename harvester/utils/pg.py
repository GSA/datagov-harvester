import psycopg

# ruff: noqa: F841


class PostgresUtility:
    def __init__(self, conn):
        self.conn = conn
        self.table = "harvestjob"

    def query(self, query):
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                self.conn.commit()
        except (Exception, psycopg.Error) as error:
            print("Query Error:", error)

    def close(self):
        self.conn.close()

    def store_new_entry(self, job_id):
        # TODO verify query
        self.query(
            f"""
        INSERT INTO {self.table} (jobid, status)
            VALUES ('{job_id}', 'new');
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
