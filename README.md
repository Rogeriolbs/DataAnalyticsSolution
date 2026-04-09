# DataAnalyticsSolution — End-to-End Data Analytics Solution for Retail

A portfolio project showcasing a production-grade, end-to-end Data Analytics platform built for a retail company using the modern data stack. This project demonstrates best practices across **Data Engineering**, **Data Analysis**, **Data Science**, **Data Governance**, **FinOps**, and **DataOps** disciplines.

---

## Project Goal

Build and document a robust, cloud-native analytics solution from raw data ingestion to AI-powered business insights — deployable across multiple cloud providers — to showcase real-world expertise in designing, implementing, and operating modern data platforms.

---

## Architecture Overview

### Data Lakehouse — Medallion Architecture

| Layer | Description |
|---|---|
| **Landing Zone** | Raw file drop zone (CSV, JSON, Parquet, etc.) — no transformation |
| **Bronze** | Data loaded as-is from Landing Zone; preserves raw history |
| **Silver** | Cleansed, conformed data modeled as a **Snowflake Schema** with **SCD Type 2** on key dimension tables |
| **Gold** | Consumption-ready **Star Schema** and **One Big Table (OBT)** for self-service analytics |

### Data Pipelines

- Built in **Python / PySpark**
- **Schema enforcement and evolution** — contract-based ingestion with automatic schema drift handling
- **Data quality checks** — expectation-based validation at each layer transition
- **Monitoring** — pipeline observability with alerting on quality failures

### Orchestration

- **Databricks Lakeflow** — native orchestration for Databricks workloads
- **Apache Airflow** — DAG-based scheduling for cross-platform and external dependencies

### CI/CD & Deployment

- **GitHub Actions** — automated testing, linting, and deployment pipelines
- **Databricks Asset Bundles (DAB)** — infrastructure-as-code for Databricks resources
- **Environments**: automated promotion through `DEV → UAT → PROD`
- **Multi-cloud deployment**: Databricks on **AWS**, **Azure**, and **GCP**

### AI / BI Stack

| Deliverable | Description |
|---|---|
| **Data Quality Dashboard** | Streamlit app for monitoring pipeline health, quality metrics, and anomaly alerts |
| **Business Metrics Dashboard** | Streamlit app with key retail KPIs — sales, inventory, customer behavior, and trends |
| **Agentic AI** | Conversational agent for **Text-to-SQL** queries and natural language business insights powered by LLMs |

---

## Principles & Practices

- **Data Engineering** — robust pipeline design, idempotency, incremental loads, partitioning
- **Data Governance** — lineage tracking, schema contracts, data cataloging, access control
- **DataOps** — version-controlled pipelines, automated testing, environment promotion
- **FinOps** — cloud cost visibility, compute optimization, resource tagging strategy

---

## Tech Stack

| Category | Technologies |
|---|---|
| Processing | PySpark, Python |
| Lakehouse | Databricks, Delta Lake |
| Orchestration | Databricks Lakeflow, Apache Airflow |
| CI/CD | GitHub Actions, Databricks Asset Bundles (DAB) |
| Cloud | AWS, Azure, GCP |
| BI / Apps | Streamlit |
| AI | LLM-based Agentic AI (Text-to-SQL, insights) |
| Storage | Delta Lake, cloud object storage (S3 / ADLS / GCS) |

---

## Repository Structure

```
DataAnalyticsSolution/
├── landing/                  # Raw file samples and schema contracts
│   ├── sales/                # Sales transaction CSVs by date
│   ├── customers/            # Customer master data
│   ├── products/             # Product catalog
│   ├── stores/               # Store master data
│   ├── inventory/            # Inventory snapshots
│   └── schemas/              # JSON Schema contracts per source
├── pipelines/                # PySpark transformation pipelines
│   ├── common/               # Shared utilities (config, Spark session, quality, logger)
│   ├── bronze/               # Landing → Bronze ingestion pipeline
│   ├── silver/               # Bronze → Silver Snowflake Schema + SCD Type 2
│   ├── gold/                 # Silver → Gold Star Schema, OBT, and KPI aggregations
│   └── run_pipeline.py       # End-to-end pipeline runner
├── dags/                     # Apache Airflow DAGs
├── bundles/                  # Databricks Asset Bundle (DAB) definitions
├── dashboards/               # Streamlit applications
├── agents/                   # Agentic AI (Text-to-SQL, insights)
├── tests/                    # Unit and integration tests
│   └── unit/
│       ├── bronze/
│       └── silver/
├── .github/                  # GitHub Actions CI/CD workflows
└── docs/                     # Architecture diagrams and documentation
```

---

## 1. Install and Build

### Prerequisites

| Requirement | Minimum Version |
|---|---|
| Python | 3.10+ |
| Java (JDK) | 11+ (required by PySpark) |
| Git | any recent version |

### Clone the repository

```bash
git clone https://github.com/Rogeriolbs/DataAnalyticsSolution.git
cd DataAnalyticsSolution
```

### Create and activate a virtual environment

```bash
# Create
python -m venv .venv

# Activate — Linux / macOS
source .venv/bin/activate

# Activate — Windows (PowerShell)
.venv\Scripts\Activate.ps1

# Activate — Windows (Git Bash)
source .venv/Scripts/activate
```

### Install dependencies

```bash
# Core pipeline dependencies (PySpark, Delta Lake, Great Expectations)
pip install -e ".[dev]"

# Also install dashboard dependencies (optional)
pip install -e ".[dashboards]"

# Also install AI agent dependencies (optional)
pip install -e ".[agents]"
```

### Configure environment variables

```bash
# Copy the example file and fill in your values
cp .env.example .env
```

Edit `.env` with your storage paths and Databricks credentials:

```env
ENVIRONMENT=dev
LANDING_PATH=/mnt/landing        # or a local path for local runs
BRONZE_PATH=/mnt/bronze
SILVER_PATH=/mnt/silver
GOLD_PATH=/mnt/gold
DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
DATABRICKS_TOKEN=<your-pat>
```

### Run the tests

```bash
pytest
```

---

## 2. Generate Sample Data Files

The `landing/` folder already contains realistic retail CSV samples for local development and testing. No generation step is required — the files are committed to the repository.

### Available sample files

| Entity | File | Rows |
|---|---|---|
| Sales | `landing/sales/sales_20240101.csv` | 15 transactions |
| Sales | `landing/sales/sales_20240102.csv` | 10 transactions |
| Customers | `landing/customers/customers_20240101.csv` | 22 customers |
| Products | `landing/products/products_20240101.csv` | 23 products |
| Stores | `landing/stores/stores_20240101.csv` | 6 stores |
| Inventory | `landing/inventory/inventory_20240101.csv` | 20 records |

### Schema contracts

Each source has a JSON Schema contract in `landing/schemas/` that defines required fields, types, and allowed values. The Bronze pipeline validates incoming files against these contracts and:

- **Halts** the pipeline on breaking changes (missing required columns)
- **Warns** on additive drift (new columns not in the contract)

To add new sample files, drop additional CSVs following the same naming convention (`<entity>_YYYYMMDD.csv`) into the corresponding `landing/<entity>/` folder.

---

## 3. Deploy the Solution

### 3.1 Local (Development)

Run pipelines locally using a local SparkSession and filesystem paths. This mode is intended for development and unit testing — no Databricks cluster is required.

```bash
# Set local paths in .env
LANDING_PATH=./landing
BRONZE_PATH=./spark-warehouse/bronze
SILVER_PATH=./spark-warehouse/silver
GOLD_PATH=./spark-warehouse/gold

# Run the full pipeline locally
python -m pipelines.run_pipeline --layer all --project-root .
```

> Delta tables will be written to `./spark-warehouse/` on your local filesystem.

---

### 3.2 Databricks on Azure

**Prerequisites:** Databricks workspace on Azure, ADLS Gen2 storage account, Service Principal with Storage Blob Data Contributor role.

#### Step 1 — Install the Databricks CLI

```bash
pip install databricks-cli
# or
winget install Databricks.DatabricksCLI
```

#### Step 2 — Configure the CLI

```bash
databricks configure --token
# Enter your workspace URL and Personal Access Token
```

#### Step 3 — Mount storage (or use Unity Catalog volumes)

In your Databricks workspace, mount your ADLS Gen2 container:

```python
# Run in a Databricks notebook
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "<service-principal-client-id>",
    "fs.azure.account.oauth2.client.secret": "<service-principal-secret>",
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant-id>/oauth2/token",
}
dbutils.fs.mount(
    source="abfss://<container>@<storage-account>.dfs.core.windows.net/",
    mount_point="/mnt/data",
    extra_configs=configs,
)
```

#### Step 4 — Set environment variables

```env
ENVIRONMENT=dev
LANDING_PATH=/mnt/data/landing
BRONZE_PATH=/mnt/data/bronze
SILVER_PATH=/mnt/data/silver
GOLD_PATH=/mnt/data/gold
```

#### Step 5 — Deploy with Databricks Asset Bundles

```bash
# Validate the bundle configuration
databricks bundle validate --target dev

# Deploy to DEV
databricks bundle deploy --target dev

# Deploy to UAT
databricks bundle deploy --target uat

# Deploy to PROD
databricks bundle deploy --target prod
```

---

### 3.3 Databricks on AWS

**Prerequisites:** Databricks workspace on AWS, S3 bucket, IAM role with S3 read/write permissions attached to the Databricks instance profile.

#### Step 1 — Configure instance profile in Databricks

Attach an IAM instance profile to your Databricks cluster that grants access to your S3 bucket. See the [Databricks AWS documentation](https://docs.databricks.com/administration-guide/cloud-configurations/aws/instance-profiles.html) for details.

#### Step 2 — Set environment variables

```env
ENVIRONMENT=dev
LANDING_PATH=s3a://<your-bucket>/landing
BRONZE_PATH=s3a://<your-bucket>/bronze
SILVER_PATH=s3a://<your-bucket>/silver
GOLD_PATH=s3a://<your-bucket>/gold
```

#### Step 3 — Deploy with Databricks Asset Bundles

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

---

### 3.4 Databricks on GCP

**Prerequisites:** Databricks workspace on GCP, GCS bucket, GCP Service Account with Storage Object Admin role.

#### Step 1 — Configure GCS credentials

In your Databricks cluster configuration, add the GCS service account key as a Spark config:

```
spark.hadoop.fs.gs.auth.service.account.enable true
spark.hadoop.fs.gs.auth.service.account.email <service-account>@<project>.iam.gserviceaccount.com
spark.hadoop.fs.gs.auth.service.account.private.key.id <key-id>
spark.hadoop.fs.gs.auth.service.account.private.key <private-key>
```

#### Step 2 — Set environment variables

```env
ENVIRONMENT=dev
LANDING_PATH=gs://<your-bucket>/landing
BRONZE_PATH=gs://<your-bucket>/bronze
SILVER_PATH=gs://<your-bucket>/silver
GOLD_PATH=gs://<your-bucket>/gold
```

#### Step 3 — Deploy with Databricks Asset Bundles

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

---

## 4. Run the Pipelines

All pipelines are driven by [`pipelines/run_pipeline.py`](pipelines/run_pipeline.py), which executes each layer in sequence and runs data quality checks after every stage.

### Run all layers (end-to-end)

```bash
python -m pipelines.run_pipeline --layer all
```

### Run a single layer

```bash
# Bronze only — ingest from Landing Zone
python -m pipelines.run_pipeline --layer bronze

# Silver only — transform Bronze → Silver (Snowflake Schema + SCD Type 2)
python -m pipelines.run_pipeline --layer silver

# Gold only — build Star Schema, OBT, and KPI aggregations
python -m pipelines.run_pipeline --layer gold
```

### Run with a custom project root (local mode)

```bash
python -m pipelines.run_pipeline --layer all --project-root /path/to/DataAnalyticsSolution
```

### What each layer produces

| Layer | Output tables |
|---|---|
| **Bronze** | `bronze/sales`, `bronze/customers`, `bronze/products`, `bronze/stores`, `bronze/inventory` |
| **Silver** | `fact_sales`, `dim_date`, `dim_store`, `dim_customer` (SCD2), `dim_product` (SCD2), `dim_inventory` |
| **Gold** | `fact_sales`, `dim_*`, `obt_sales`, `agg_daily_sales`, `agg_monthly_sales` |

### Data quality checks

Quality checks run automatically after each layer. Rules are defined in:

- [`pipelines/bronze/quality_checks.py`](pipelines/bronze/quality_checks.py)
- [`pipelines/silver/quality_checks.py`](pipelines/silver/quality_checks.py)
- [`pipelines/gold/quality_checks.py`](pipelines/gold/quality_checks.py)

Rules have two severity levels:

| Severity | Behavior |
|---|---|
| `CRITICAL` | Pipeline halts immediately on failure |
| `WARNING` | Failure is logged but the pipeline continues |

### Running individual pipelines directly

```bash
# Bronze ingestion — all sources
python -m pipelines.bronze.ingest

# Silver transformation
python -m pipelines.silver.transform

# Gold transformation
python -m pipelines.gold.transform
```

---

## Status

> Project under active development. Layers and components are built iteratively. See the [GitHub Project board](https://github.com/users/Rogeriolbs/projects/1) for current progress.
