def get_terminate_processes_sql_cmd():
  """
  USE WITH CAUTION!
  
  - sql command for terminating all 'active' and 'idle in transaction' db processes.
  - use this when your migration requires an "accessExclusiveLock"
  """
  return (
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() "
        "AND state = 'active' or state = 'idle in transaction'"
    )