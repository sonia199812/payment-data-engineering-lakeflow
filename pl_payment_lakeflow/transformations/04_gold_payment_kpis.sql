CREATE OR REFRESH MATERIALIZED VIEW gold_payment_kpis
COMMENT 'Gold: Payment KPIs calculated from deduplicated transactions'
AS
SELECT
    COUNT(*) AS total_transactions,
    SUM(amount) AS total_payment_amount,
    SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END)
        AS fraudulent_transactions,
    ROUND(
        100.0 * SUM(CASE WHEN is_fraud = 1 THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        2
    ) AS fraud_rate_percentage
FROM payment_catalog.payment_de.silver_payments_deduplicated;