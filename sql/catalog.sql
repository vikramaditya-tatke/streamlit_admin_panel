SELECT
    database AS Database,
    name AS Table,
    engine AS Engine,
    total_rows AS Rows,
    total_bytes AS `Disk bytes`,
    (database, name) IN (
        SELECT database, table FROM system.columns WHERE name = 'Event Count'
    ) AS `Has event count`
FROM system.tables
WHERE database NOT IN ('information_schema', 'INFORMATION_SCHEMA')
    AND NOT startsWith(name, '.inner')
ORDER BY database, name
