SELECT
    database AS Database,
    table AS Table,
    sum(data_compressed_bytes) AS `Compressed bytes`,
    sum(data_uncompressed_bytes) AS `Uncompressed bytes`,
    count() AS `Active parts`
FROM system.parts
WHERE active = 1
GROUP BY database, table
