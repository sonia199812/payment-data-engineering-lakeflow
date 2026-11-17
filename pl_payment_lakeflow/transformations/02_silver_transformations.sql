-- STEP 1: Standardize and validate payments

CREATE OR REFRESH MATERIALIZED VIEW silver_payments_validated
COMMENT 'Silver: Standardized payments with quality checks'
AS
WITH combined_payments AS (
    SELECT
        trans_num,
        amt,
        trans_date_trans_time,
        merchant,
        category,
        is_fraud,
        ingestion_timestamp
    FROM payment_catalog.payment_de.bronze_payments

    UNION ALL

    SELECT
        trans_num,
        transaction.amount AS amt,
        trans_date_trans_time,
        transaction.merchant AS merchant,
        transaction.category AS category,
        is_fraud,
        ingestion_timestamp
    FROM payment_catalog.payment_de.bronze_payments_json
),
prepared AS (
    SELECT
        TRIM(trans_num) AS transaction_id,
        TRY_CAST(amt AS DECIMAL(18,2)) AS amount,
        TRY_CAST(trans_date_trans_time AS TIMESTAMP) AS transaction_timestamp,
        TRIM(merchant) AS merchant,
        TRIM(category) AS category,
        TRY_CAST(is_fraud AS INT) AS is_fraud,
        ingestion_timestamp
    FROM combined_payments
)
SELECT *,
    CASE
        WHEN transaction_id IS NULL OR transaction_id = ''
            THEN 'MISSING_TRANSACTION_ID'
        WHEN amount IS NULL
            THEN 'INVALID_AMOUNT'
        WHEN amount <= 0
            THEN 'NON_POSITIVE_AMOUNT'
        WHEN transaction_timestamp IS NULL
            THEN 'INVALID_TIMESTAMP'
        WHEN is_fraud IS NULL OR is_fraud NOT IN (0, 1)
            THEN 'INVALID_FRAUD_LABEL'
        ELSE NULL
    END AS rejection_reason
FROM prepared;

-- STEP 2: Store invalid payments

CREATE OR REFRESH MATERIALIZED VIEW silver_payments_rejected
COMMENT 'Silver: Invalid payment transactions'
AS
SELECT *
FROM payment_catalog.payment_de.silver_payments_validated
WHERE rejection_reason IS NOT NULL;


-- STEP 3: Keep one valid record per transaction ID

CREATE OR REFRESH MATERIALIZED VIEW silver_payments_deduplicated
COMMENT 'Silver: Valid unique payment transactions'
AS
SELECT
    transaction_id,
    amount,
    transaction_timestamp,
    merchant,
    category,
    is_fraud,
    ingestion_timestamp
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY ingestion_timestamp DESC,
                     transaction_timestamp DESC,
                     amount DESC,
                     merchant DESC,
                     category DESC,
                     is_fraud DESC
        ) AS rn
    FROM payment_catalog.payment_de.silver_payments_validated
    WHERE rejection_reason IS NULL
)
WHERE rn = 1;