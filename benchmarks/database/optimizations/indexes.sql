-- composite index for “latest harvest record” queries
CREATE INDEX CONCURRENTLY ix_hr_source_status_identifier_created_desc
ON harvest_record (
  harvest_source_id,
  status,
  identifier,
  date_created DESC
);

-- index on job id for record errors
CREATE INDEX CONCURRENTLY ix_hre_job_id
ON harvest_record_error (harvest_job_id);