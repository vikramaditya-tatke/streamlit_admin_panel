SELECT
    {table:String} AS Table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    sum(data_compressed_bytes) / nullIf(sum(data_uncompressed_bytes), 0) AS ratio,
    (1 - ratio) * 100 AS physical_compression
FROM system.parts
WHERE database = {database:String} AND table = {table:String} AND active = 1
