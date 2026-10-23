SELECT
    {table:String} AS Table,
    sum(`Event Count`) AS event_count,
    count() AS row_count,
    row_count / nullIf(event_count, 0) AS ratio,
    (1 - ratio) * 100 AS logical_compression
FROM {database:Identifier}.{table:Identifier}
