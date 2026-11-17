# Payment Transaction & Fraud Analytics | End-to-End Azure Databricks Data Engineering Project
An end-to-end **Azure Databricks Lakehouse** project that ingests payment transactions from Azure Data Lake Storage Gen2, standardizes CSV and nested JSON sources, enforces data quality, removes duplicate transactions, and produces reconciled payment and fraud KPIs using **Lakeflow Spark Declarative Pipelines**.The project focuses on production-oriented data engineering: **incremental ingestion, multi-format onboarding, data-quality quarantine, idempotent reporting, orchestration, freshness monitoring, and volume-spike testing**.

## 1. Business problem

Payment systems may deliver files in different formats and at unpredictable volumes. Late or missing batches, invalid payment amounts, and duplicate transaction IDs can lead to incomplete reporting or overstated financial KPIs. This pipeline preserves raw source data, applies consistent validation rules, and exposes a trusted reporting layer.

## 2. Architecture
<img width="2142" height="1146" alt="architecture" src="https://github.com/user-attachments/assets/50c57818-c1be-4e57-89de-dcaa6ce5cf8f" />


## 3. Technology stack

| Area | Technology |
|---|---|
| Cloud storage | Azure Data Lake Storage Gen2 |
| Data platform | Azure Databricks, Unity Catalog, Delta tables |
| Ingestion | Auto Loader via `STREAM read_files()` |
| Processing | Lakeflow Spark Declarative Pipelines, Databricks SQL |
| Data modeling | Bronze / Silver / Gold (medallion architecture) |
| Orchestration | Lakeflow Jobs |
| Monitoring | Pipeline run history and Databricks SQL freshness alert |
| Version control | GitHub feature branch and pull-request workflow |

## 4. Dataset and pipeline design

### Bronze: preserve raw source records

- **`bronze_payments`** ingests CSV files incrementally from the source volume.
- **`bronze_payments_json`** ingests newline-delimited nested JSON from a separate JSON directory.
- Ingestion records source filename and ingestion timestamp.
- The JSON source includes nested `transaction` and `payment_details` objects and a `tags` array. We extract nested struct fields with dot notation; we do **not** explode `tags` because that would multiply payment rows.
- JSON ingestion is configured with `schemaEvolutionMode => 'rescue'`. This is **not** a claim that an unexpected-column evolution event was tested.

### Silver: standardize, validate, quarantine, deduplicate

- **`silver_payments_validated`** maps `transaction.amount`, `transaction.merchant`, and `transaction.category` to the CSV-aligned fields, combines the sources with `UNION ALL`, and converts types with `TRY_CAST`.
- Quality rules flag missing transaction IDs, invalid or nonpositive amounts, invalid timestamps, and invalid fraud flags.
- **`silver_payments_rejected`** retains rows with a `rejection_reason` for investigation.
- **`silver_payments_deduplicated`** retains one valid row per `transaction_id` using `ROW_NUMBER()`. The selection prioritizes latest ingestion timestamp and then deterministic tie-break columns. In a production CDC system, source version/event time would be preferable to ingestion time when available.

### Gold: trusted business metrics

**`gold_payment_kpis`** aggregates only `silver_payments_deduplicated`:

- Unique valid transaction count
- Total payment amount
- Fraudulent transaction count
- Fraud rate (%)

Gold reads the deduplicated Silver dataset rather than raw or merely validated records, preventing repeated payment IDs from inflating reporting metrics.

## 5. Implemented scenarios and evidence

| Scenario | Test / design | Outcome and limitation |
|---|---|---|
| 1. Initial ingestion | 100-row CSV batch | Ingested successfully |
| 2. Incremental ingestion | Second 100-row CSV batch | Ingested without intentionally reloading previous source files |
| 3. Sudden volume spike | 10,000-row CSV batch after 100-row batches | Pipeline succeeded at 100× prior batch size; no throughput benchmark claimed |
| 4. Missing hourly data | Run without a new source file; freshness SQL alert | No-new-file update succeeded; alert query tested, but scheduled email delivery not verified |
| 5. New JSON format and invalid amount | 100 nested JSON records; one nonnumeric amount | JSON Bronze ingested 100 rows; `TRY_CAST` identified the invalid amount; combined Silver rejection counts validated |
| 6. Duplicate payments / KPIs | Duplicate-ID CSV test | Validated dataset had 10,399 rows and 10,299 distinct IDs; deduplicated Silver had 10,299 rows. The observed 100 duplicate occurrences differed from the planned 50, so source-level reconciliation remains a follow-up |
| 7. Latency / cost efficiency | Triggered serverless pipeline, incremental ingestion, run-duration review | Architecture and observed durations documented; no controlled cost-savings or performance-improvement benchmark |

**Additional orchestration test:** Lakeflow Job `job_payment_hourly_orchestration`, task `run_payment_lakeflow`, successfully invoked the pipeline manually.

## 6. Validated KPI results

The Gold results matched a direct aggregation of the deduplicated Silver table:

| KPI | Reconciled value |
|---|---:|
| Unique valid transactions | **10,299** |
| Total payment amount | **706,075.90** |
| Fraudulent transactions | **48** |
| Fraud rate | **0.47%** |

**Data-quality results at the time of validation:** 100 rejected rows (99 missing transaction IDs and one invalid amount). The 99 missing-ID records were not fully traced to their original source files.

## 7. Reliability, latency, and cost decisions

- **Incremental file discovery:** Auto Loader tracks ingested files; normal pipeline updates avoid intentionally reprocessing previously ingested files.
- **Separate source readers:** CSV and JSON have independent parsing configurations and can be onboarded without changing the original CSV Bronze reader.
- **Triggered serverless execution:** Compute is provisioned for updates rather than kept running as a continuous pipeline. Scheduled runs may still incur cost even with no new files.
- **Data freshness:** A SQL alert compares the latest Bronze ingestion timestamp with a one-hour threshold. A successful pipeline update with zero new files does not mean upstream delivery was healthy. Alert scheduling and email delivery still require verification.
- **Reporting correctness:** Gold is derived from deduplicated valid records, with Silver-to-Gold reconciliation.
- **Performance boundaries:** A 100× batch-size spike completed successfully; no claim is made about automatic scale-out observed, optimized query plans, lower latency, or quantified cost savings.

## 8. Repository layout

```text
payment-data-engineering-lakeflow/
├── README.md
├── transformations/
│   ├── 01_bronze_ingestion.sql
│   ├── 02_silver_transformations.sql
│   ├── 03_bronze_json_ingestion.sql
│   └── 04_gold_payment_kpis.sql
└── docs/
    ├── architecture.md
    └── validation-and-scenarios.md
```

The SQL files above should be committed from the **actual tested Databricks pipeline definitions**, not reconstructed from documentation. No storage keys, tokens, credentials, or personal data should be committed.

## 9. Execution and validation

1. Provision access to the source directories through Unity Catalog and verify the external volume path.
2. Configure the Lakeflow pipeline to include all four SQL source files.
3. Run a **normal update** to process new files; avoid full refresh unless deliberately rebuilding state.
4. Validate Bronze counts, Silver rejection reasons and distinct IDs, and Gold-to-Silver aggregates.
5. Trigger the pipeline using the Lakeflow Job and inspect task and pipeline run histories.
6. Check alert schedule and notifications before relying on automated freshness detection.

Example reconciliation:

```sql
SELECT
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_payment_amount,
    COUNT_IF(is_fraud = 1) AS fraudulent_transactions,
    ROUND(100.0 * COUNT_IF(is_fraud = 1) / NULLIF(COUNT(*), 0), 2)
        AS fraud_rate_percentage
FROM payment_catalog.payment_de.silver_payments_deduplicated;
```

## 10. Engineering trade-offs and future work

- **Schema rescue vs schema evolution:** Rescue mode is configured; adding an unexpected JSON column in a later batch and verifying rescued data is a future test.
- **Duplicate semantics:** Ingestion timestamp is a practical tie-breaker for this demonstration; production payment corrections may require event versioning, CDC, and an explicit conflict-resolution policy.
- **Monitoring:** Verify hourly alert scheduling, email delivery, and escalation. A freshness alert detects stale ingested data but does not automatically diagnose the upstream cause.
- **Scale:** Investigate query profiles, shuffle/skew, incremental MV refresh behavior, and billable usage before making optimization claims.
- **Dashboards:** A BI dashboard is optional; the current deliverable is the validated Gold reporting dataset.
