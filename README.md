# PipelineSandbox — End-to-End Data Analytics Solution for Retail

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

## Repository Structure *(evolving)*

```
PipelineSandbox/
├── landing/          # Raw file samples and ingestion configs
├── pipelines/        # PySpark transformation pipelines (Bronze → Silver → Gold)
├── dags/             # Airflow DAGs
├── bundles/          # Databricks Asset Bundle definitions
├── dashboards/       # Streamlit applications
├── agents/           # Agentic AI (Text-to-SQL, insights)
├── tests/            # Unit and integration tests
├── .github/          # GitHub Actions CI/CD workflows
└── docs/             # Architecture diagrams and documentation
```

---

## Status

> Project under active development. Layers and components will be built iteratively and documented as they are completed.
