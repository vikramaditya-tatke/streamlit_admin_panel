SELECT
    sum(data_compressed_bytes),
    sum(data_uncompressed_bytes),
    formatReadableSize(sum(data_compressed_bytes)),
    formatReadableSize(sum(data_uncompressed_bytes))
FROM system.parts
WHERE database = {database:String} AND table = {table:String} AND active
