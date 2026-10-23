SELECT
    name,
    name IN (
        SELECT table
        FROM system.columns
        WHERE database = {database:String} AND name = 'Event Count'
    ) AS has_event_count
FROM system.tables
WHERE database = {database:String}
    AND engine LIKE '%MergeTree'
    AND name NOT ILIKE '%MV'
    AND NOT startsWith(name, '.inner')
ORDER BY name
