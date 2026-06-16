-- get the successful change log count and number of dataset per harvest source
-- from this list get something from the top, middle, and bottom (large, medium, small)
SELECT
    harvest_source_id,
    COUNT(*) FILTER (
        WHERE status = 'success'
    ) AS successful_historical_rows,
    COUNT(DISTINCT identifier) FILTER (
        WHERE status = 'success'
    ) AS successful_identifiers
FROM harvest_record
GROUP BY harvest_source_id
ORDER BY successful_historical_rows DESC;

-- get jobs ordered by record error count
SELECT
    hj.id,
    COUNT(hre.id) AS record_error_count
FROM harvest_job AS hj
JOIN harvest_record_error AS hre
    ON hre.harvest_job_id = hj.id
GROUP BY hj.id
ORDER BY
    record_error_count ASC,
    hj.date_created DESC;

-- get harvest sources ordered by job count
SELECT
    hs.id as harvest_source_id,
    count(*) as dataset_count
FROM harvest_source AS hs
JOIN dataset as d
    on hs.id = d.harvest_source_id
GROUP BY hs.id
ORDER BY dataset_count DESC;

-- EXPLAIN (ANALYZE, BUFFERS, COSTS, VERBOSE)   
-- SELECT dataset.slug, dataset.dcat, dataset.translated_spatial, dataset.organization_id, dataset.harvest_source_id, dataset.harvest_record_id, dataset.popularity, dataset.last_harvested_date, dataset.id, organization_1.name, organization_1.logo, organization_1.description, organization_1.slug AS slug_1, organization_1.organization_type, organization_1.aliases, organization_1.id AS id_1, (SELECT count(harvest_source.id) AS count_1 FROM harvest_source WHERE harvest_source.organization_id = organization_1.id) AS anon_1, harvest_source_1.organization_id AS organization_id_1, harvest_source_1.name AS name_1, harvest_source_1.url, harvest_source_1.notification_emails, harvest_source_1.frequency, harvest_source_1.schema_type, harvest_source_1.source_type, harvest_source_1.notification_frequency, harvest_source_1.collection_parent_url, harvest_source_1.id AS id_2, organization_2.name AS name_2, organization_2.logo AS logo_1, organization_2.description AS description_1, organization_2.slug AS slug_2, organization_2.organization_type AS organization_type_1, organization_2.aliases AS aliases_1, organization_2.id AS id_3, (SELECT count(harvest_source.id) AS count_1 FROM harvest_source WHERE harvest_source.organization_id = organization_2.id) AS anon_2, harvest_record_1.identifier, harvest_record_1.harvest_job_id, harvest_record_1.harvest_source_id AS harvest_source_id_1, harvest_record_1.source_hash, harvest_record_1.source_raw, harvest_record_1.source_transform, harvest_record_1.date_created, harvest_record_1.date_finished, harvest_record_1.ckan_id, harvest_record_1.action, harvest_record_1.parent_identifier, harvest_record_1.status, harvest_record_1.id AS id_4 FROM dataset LEFT OUTER JOIN organization AS organization_1 ON organization_1.id = dataset.organization_id LEFT OUTER JOIN harvest_source AS harvest_source_1 ON harvest_source_1.id = dataset.harvest_source_id LEFT OUTER JOIN organization AS organization_2 ON organization_2.id = harvest_source_1.organization_id LEFT OUTER JOIN harvest_record AS harvest_record_1 ON harvest_record_1.id = dataset.harvest_record_id WHERE dataset.harvest_source_id = '9996b8ab-2032-4fd4-acfd-2702991ae5ef' ORDER BY dataset.last_harvested_date DESC NULLS LAST  LIMIT 10 OFFSET 0;