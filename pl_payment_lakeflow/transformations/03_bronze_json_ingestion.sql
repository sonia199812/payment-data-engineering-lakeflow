CREATE OR REFRESH STREAMING TABLE bronze_payments_json
COMMENT 'Bronze: Incremental ingestion of nested JSON payments'
AS
SELECT
    *,
    _metadata.file_name AS source_file,
    current_timestamp() AS ingestion_timestamp
FROM STREAM read_files(
    '/Volumes/payment_catalog/payment_de/payment_landing/json/',
    format => 'json',
    schemaEvolutionMode => 'rescue'
);