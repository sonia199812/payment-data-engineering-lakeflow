
CREATE OR REFRESH STREAMING TABLE bronze_payments
COMMENT 'Bronze: Raw payment transactions ingested incrementally'
AS
SELECT
    *,
    _metadata.file_name AS source_file,
    current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
    '/Volumes/payment_catalog/payment_de/payment_landing/',
    format => 'csv',
    header => true,
    inferColumnTypes => false,
    schemaEvolutionMode => 'rescue'
);
