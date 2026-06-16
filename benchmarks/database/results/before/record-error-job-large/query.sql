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
WHERE harvest_record_error.harvest_job_id = '02c5d53a-dc97-4597-b854-cc07203506da'