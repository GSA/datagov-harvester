SELECT anon_1.identifier, 
  anon_1.source_hash, 
  anon_1.ckan_id, 
  anon_1.date_created, 
  anon_1.date_finished, 
  anon_1.id, 
  anon_1.action, 
  dataset.slug AS dataset_slug 
FROM (
    SELECT DISTINCT ON (harvest_record.identifier) 
      harvest_record.identifier AS identifier, 
      harvest_record.harvest_job_id AS harvest_job_id, 
      harvest_record.harvest_source_id AS harvest_source_id, 
      harvest_record.source_hash AS source_hash, 
      harvest_record.source_raw AS source_raw, 
      harvest_record.source_transform AS source_transform, 
      harvest_record.date_created AS date_created, 
      harvest_record.date_finished AS date_finished, 
      harvest_record.ckan_id AS ckan_id, 
      harvest_record.action AS action, 
      harvest_record.parent_identifier AS parent_identifier, 
      harvest_record.status AS status, 
      harvest_record.id AS id 
    FROM harvest_record 
    WHERE harvest_record.status = 'success' 
      AND harvest_record.harvest_source_id = 'a73d251c-f44e-4f76-8bf3-d8d45876adb2'
    ORDER BY harvest_record.identifier, harvest_record.date_created DESC
) AS anon_1 
LEFT OUTER JOIN dataset 
  ON dataset.harvest_record_id = anon_1.id 
WHERE anon_1.action != 'delete';