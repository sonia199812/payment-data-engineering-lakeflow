# Azure Databricks Payment Transaction & Fraud Analytics | End-to-End Data Engineering Project

An end-to-end **Azure Data Engineering project** built using **Azure Databricks, Azure Data Lake Storage Gen2, Lakeflow Spark Declarative Pipelines, Databricks SQL, Delta Lake, and Medallion Architecture**.

The project implements an incremental payment data pipeline that ingests CSV and nested JSON transactions, validates data quality, isolates invalid records, removes duplicate transaction IDs, and produces reliable payment and fraud analytics.

## 1. Business Problem

Payment processing systems generate transaction data from multiple sources and formats. These datasets may contain missing transaction identifiers, invalid payment amounts, duplicate transactions, and delayed file arrivals.

Without proper data validation and deduplication, these issues can result in inaccurate transaction counts, overstated payment amounts, and unreliable fraud reporting.

**Solution:** Build a Lakehouse pipeline that ingests payment transactions incrementally, preserves raw data, applies quality rules, isolates rejected records, deduplicates valid transactions, and generates reconciled business metrics.

## 2. Project Objectives

- Implement Bronze, Silver, and Gold layers using Medallion Architecture.
- Ingest CSV and nested JSON files from Azure Data Lake Storage Gen2.
- Use Databricks Auto Loader for incremental file ingestion.
- Develop SQL-based transformations using Lakeflow Spark Declarative Pipelines.
- Identify invalid payment records and retain rejection reasons.
- Deduplicate valid transactions using transaction IDs.
- Generate payment and fraud KPIs.
- Validate record counts and reconcile Gold metrics against Silver.
- Test ingestion, data quality, duplicate handling, data freshness, and higher-volume batches.
- Orchestrate pipeline execution using Lakeflow Jobs.

## 3. Project Architecture

<img width="1754" height="897" alt="Payment Analytics End-to-End Flow (2)" src="https://github.com/user-attachments/assets/39c8e24e-0a39-4fbd-8dd6-a3a30df8d18d" />


**Pipeline:** `pl_payment_lakeflow`

**Orchestration:** `job_payment_hourly_orchestration`

**Unity Catalog:** `payment_catalog.payment_de`

The Lakeflow pipeline uses triggered serverless execution. New files are processed during pipeline updates rather than through a continuously running stream.

## 4. Technology Stack

| Component | Technology |
|---|---|
| Cloud platform | Microsoft Azure |
| Cloud storage | Azure Data Lake Storage Gen2 |
| Data processing | Azure Databricks |
| Pipeline framework | Lakeflow Spark Declarative Pipelines |
| Ingestion | Databricks Auto Loader |
| Programming language | SQL |
| Data architecture | Medallion Architecture |
| Storage format | Delta Lake |
| Governance | Unity Catalog |
| Orchestration | Lakeflow Jobs |
| Monitoring | Pipeline run history and SQL freshness checks |
| Version control | Git and GitHub |

## 5. Dataset Overview

The project processes payment transaction records from two source formats.

**CSV transactions** contain payment identifiers, transaction amounts, timestamps, fraud indicators, and other payment attributes.

**Nested JSON transactions** contain structured payment information, including `transaction`, `payment_details`, and `tags` fields.

The files are stored in Azure Data Lake Storage Gen2 and accessed through a Unity Catalog external volume.

Both formats are standardized into a common Silver-layer transaction model.

## 6. Pipeline Implementation
<img width="740" height="233" alt="image" src="https://github.com/user-attachments/assets/2ba61470-efd1-46e6-8be1-9b075925002c" />

### Bronze Layer — Incremental Ingestion

**Streaming tables:**

- `bronze_payments`
- `bronze_payments_json`

The Bronze layer uses Auto Loader through `STREAM read_files()` to discover and ingest incoming files.

CSV and JSON sources have separate ingestion definitions to accommodate their different parsing requirements.

The ingestion process preserves source records and captures file metadata and ingestion timestamps.

The JSON ingestion definition includes schema rescue configuration.

### Silver Layer — Data Quality and Deduplication

**Materialized views:**

- `silver_payments_validated`
- `silver_payments_rejected`
- `silver_payments_deduplicated`

**Validation and standardization**

CSV and JSON records are mapped to a common schema using SQL transformations and `UNION ALL`.

`TRY_CAST()` is used for safe data-type conversion, allowing malformed values to be identified without terminating the entire transformation.

The implemented quality checks identify:

- Missing transaction IDs.
- Invalid or nonpositive payment amounts.
- Invalid transaction timestamps.
- Invalid fraud indicators.

**Rejected records**

Records failing validation are retained in `silver_payments_rejected` with rejection reasons for investigation.

**Deduplication**

Valid transactions are deduplicated by `transaction_id` using `ROW_NUMBER()`.

The selection logic prioritizes the latest ingestion timestamp and uses additional tie-breaking fields.

The resulting dataset contains one valid record per transaction ID.

### Gold Layer — Payment and Fraud Analytics

**Materialized view:** `gold_payment_kpis`

The Gold layer reads `silver_payments_deduplicated` and calculates:

- Total valid unique transactions.
- Total payment amount.
- Fraudulent transaction count.
- Fraud rate percentage.

Using deduplicated Silver data prevents repeated transaction IDs from inflating financial reporting metrics.

## 7. Engineering Scenarios Tested

| Scenario | Implementation and observed result |
|---|---|
| 1. Initial ingestion | Processed an initial 100-record CSV batch using Auto Loader. |
| 2. Incremental ingestion | Processed a second 100-record CSV batch through a subsequent pipeline update. |
| 3. Sudden volume spike | Successfully processed a 10,000-record CSV batch, 100 times the initial batch size. |
| 4. Missing incoming file | Executed a pipeline update without a new source file and tested SQL-based ingestion freshness logic. |
| 5. JSON ingestion and invalid data | Ingested 100 nested JSON records and identified an invalid numeric payment amount through Silver validation. |
| 6. Duplicate transaction handling | Applied transaction-ID deduplication and validated 10,299 unique, non-null transaction IDs. |
| 7. Performance and execution efficiency | Used triggered serverless pipeline execution and reviewed pipeline run behavior during ingestion testing. |

The pipeline was also successfully invoked manually through the Lakeflow Job `job_payment_hourly_orchestration`, using task `run_payment_lakeflow`.

## 8. Data Quality Validation

The project includes a dedicated validation notebook:

`validation/payment_data_quality_validation.ipynb`

The notebook covers four core validation areas.

| Validation area | Purpose |
|---|---|
| Bronze ingestion | Verify CSV and JSON record counts |
| Silver data quality | Validate accepted and rejected records |
| Silver deduplication | Confirm unique, non-null transaction IDs |
| Gold reconciliation | Verify business KPIs against Silver |

### Record Count Reconciliation

| Processing stage | Record count |
|---|---:|
| Bronze CSV | 10,399 |
| Bronze JSON | 100 |
| **Total Bronze** | **10,499** |
| Silver validated | 10,499 |
| Invalid records | 100 |
| Valid records | 10,399 |
| Duplicate occurrences excluded | 100 |
| **Silver deduplicated** | **10,299** |

**Reconciliation:**

```text
Total Bronze = CSV Bronze + JSON Bronze
10,499 = 10,399 + 100

Valid Records = Total Bronze - Rejected Records
10,399 = 10,499 - 100

Deduplicated Records = Valid Records - Duplicate Occurrences
10,299 = 10,399 - 100

Reconciliation Difference = 0
```

### Rejected Record Analysis

| Rejection reason | Records |
|---|---:|
| Missing transaction ID | 99 |
| Invalid payment amount | 1 |
| **Total rejected** | **100** |

The Silver deduplication validation confirmed:

```text
Total Rows          : 10,299
Distinct IDs        : 10,299
Null Transaction IDs: 0
```

This confirms that the final Silver output contains no duplicate or null transaction IDs.

## 9. Gold Layer — Validated Business Results

| Business metric | Validated result |
|---|---:|
| Total valid transactions | **10,299** |
| Total payment amount | **706,075.90** |
| Fraudulent transactions | **48** |
| Fraud rate | **0.47%** |

The Gold metrics were independently recalculated from `silver_payments_deduplicated`.

All four values matched the corresponding results in `gold_payment_kpis`.

**Gold-to-Silver KPI reconciliation: Passed.**

## 10. Pipeline Orchestration and Monitoring

### Lakeflow Jobs

The project uses the Lakeflow Job `job_payment_hourly_orchestration` with task `run_payment_lakeflow` to invoke the payment pipeline.

A manual orchestration run completed successfully.

### Ingestion Freshness

A SQL freshness check compares the latest Bronze ingestion timestamp with a one-hour freshness threshold.

This helps distinguish successful pipeline execution from the absence of newly delivered source data.

### Pipeline Execution

The pipeline uses triggered serverless execution and incremental file ingestion.

Pipeline run history was used to review execution behavior during the tested ingestion scenarios.

## 11. Repository Structure

```text
payment-data-engineering-lakeflow/
|
├── config/
|
├── pl_payment_lakeflow/
│   └── transformations/
│       ├── 01_bronze_ingestion.sql
│       ├── 02_silver_transformations.sql
│       ├── 03_bronze_json_ingestion.sql
│       └── 04_gold_payment_kpis.sql
|
├── validation/
│   └── payment_data_quality_validation.ipynb
|
├── .gitignore
└── README.md
```

The repository separates pipeline transformations, configuration, and validation logic.

## 12. How to Run the Project

1. Configure access to the ADLS Gen2 payment source through the Unity Catalog external volume.
2. Ensure the required catalog and schema are available.
3. Configure the Lakeflow pipeline with the four SQL transformation files.
4. Run a triggered pipeline update to ingest available source files.
5. Verify Bronze streaming tables and Silver materialized views.
6. Run the validation notebook to check record counts, rejected records, and transaction uniqueness.
7. Compare Gold KPIs against independent Silver calculations.
8. Execute the Lakeflow Job to validate orchestration.

## 13. Project Outcomes

- Built a six-table Medallion Architecture pipeline using Azure Databricks.
- Implemented incremental CSV and nested JSON ingestion using Auto Loader.
- Standardized multiple source formats through SQL transformations.
- Implemented data-quality checks and rejection handling.
- Deduplicated valid transactions by transaction ID.
- Processed a 10,000-record batch following smaller ingestion batches.
- Validated 10,499 ingested records across CSV and JSON sources.
- Produced 10,299 unique valid transactions.
- Reconciled payment and fraud metrics between Silver and Gold.
- Executed the pipeline through Lakeflow Jobs.
- Organized transformation and validation code under Git version control.

## 14. Key Engineering Learnings

This project demonstrates practical implementation of incremental file ingestion, streaming tables, materialized views, multi-format data processing, data-quality validation, duplicate handling, Lakeflow orchestration, and business-metric reconciliation.

The pipeline preserves raw records for traceability, isolates invalid transactions before reporting, and calculates business metrics from a deduplicated Silver dataset.

The resulting Lakehouse provides a validated foundation for payment transaction and fraud analytics.

---

**GitHub Repository:** [payment-data-engineering-lakeflow](https://github.com/sonia199812/payment-data-engineering-lakeflow)
