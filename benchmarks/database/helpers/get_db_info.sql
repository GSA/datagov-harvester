-- postgres version and configuration
SELECT version();
SHOW shared_buffers;
SHOW work_mem;
SHOW random_page_cost;
SHOW effective_cache_size;

-- table sizes
SELECT
    relname AS table_name,
    n_live_tup AS num_rows,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_stat_user_tables
WHERE relname IN (
    'harvest_record',
    'harvest_job',
    'harvest_record_error',
    'harvest_source',
    'dataset'
)
ORDER BY pg_total_relation_size(relid) DESC;

-- indexes
SELECT
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename IN (
    'harvest_record',
    'harvest_job',
    'harvest_record_error',
    'harvest_source',
    'dataset'
)
ORDER BY tablename, indexname;