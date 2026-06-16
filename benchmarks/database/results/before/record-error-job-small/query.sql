SELECT harvest_record_error.harvest_record_id, 
  harvest_record_error.harvest_job_id, 
  harvest_record_error.date_created, 
  harvest_record_error.type, 
  harvest_record_error.message, 
  harvest_record_error.id, 
  harvest_record.identifier, 
  harvest_record.source_raw 
FROM harvest_record_error 
LEFT OUTER JOIN harvest_record 
ON harvest_record.id = harvest_record_error.harvest_record_id 
WHERE harvest_record_error.harvest_job_id = '3d9abdfa-2ba1-4948-8ee9-a454797c28cd'