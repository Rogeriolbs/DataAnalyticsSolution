# Product Requirements Document — DataAnalyticsSolution

> **Type:** Portfolio project  
> **Domain:** Retail analytics  
> **Author:** Rogerio Brum 
> **Status:** Active development  
> **Last updated:** 2026-04-13

---

## 1. Purpose of This Document

This PRD defines the scope, goals, user needs, and feature requirements for **DataAnalyticsSolution** — an end-to-end data analytics platform built for a fictional retail company.

It serves two purposes:

1. **Portfolio artifact** — demonstrates product thinking alongside technical implementation. Hiring managers and technical reviewers can use it to understand *why* decisions were made, not just what was built.
2. **Development compass** — keeps the scope honest, prevents scope creep, and makes trade-offs explicit so the project stays shippable.

---

## 2. Problem Statement

A mid-size retail company collects transactional data from multiple sources (POS systems, inventory management, and CRM) but cannot turn that raw data into reliable business insights. Problems include:

- Raw data is siloed in flat files with no quality enforcement.
- Analysts spend most of their time cleaning data rather than analyzing it.
- There is no single source of truth for KPIs like revenue, inventory turnover, or customer lifetime value.
- There is no self-service analytics layer — every question requires a data engineer.
- The company has no visibility into pipeline health: silent failures go undetected for days.

**DataAnalyticsSolution** solves these problems by building a production-grade data lakehouse — from raw file ingestion to AI-powered self-service analytics.

---

## 3. Goals

| Goal | Description |
|---|---|
| **G1 — Reliable data foundation** | Ingest raw retail data through a validated, idempotent pipeline that enforces schema contracts and tracks quality at every layer. |
| **G2 — Analyst-ready data model** | Produce a clean Snowflake Schema (Silver) and Star Schema / OBT (Gold) that business analysts can query without data engineering support. |
| **G3 — Operational visibility** | Surface pipeline health, data quality metrics, and anomalies through a dedicated monitoring dashboard. |
| **G4 — Business intelligence** | Deliver a self-service BI dashboard with key retail KPIs — sales trends, inventory levels, customer behavior. |
| **G5 — Conversational analytics** | Allow non-technical users to ask business questions in natural language and receive SQL-backed answers via an AI agent. |
| **G6 — Cloud portability** | Deploy the same solution on Databricks Free Tier, Databricks on Azure, AWS, and GCP with minimal environment-specific changes. |
| **G7 — Platform maturity** | Apply DataOps practices: automated testing, CI/CD, environment promotion (DEV → UAT → PROD), and infrastructure-as-code. |

### Non-Goals

- Real-time / streaming ingestion (batch only for now).
- Multi-tenant or role-based access control (single workspace).
- Production SLAs or on-call processes — this is a portfolio project.
- Building a custom orchestration engine (Lakeflow / Airflow are used as-is).

---

## 4. Users and Personas

### 4.1 Primary Personas (Fictional Retail Company)

**Business Analyst — "Ana"**
- Comfortable with Excel and basic SQL; no Python or Spark experience.
- Needs: pre-built KPI dashboards, ability to filter by date/store/product, and a way to ask ad-hoc questions without writing code.
- Success: answers business questions in under 5 minutes without filing a ticket.

**Data Engineer — "Bruno"**
- Owns the pipeline infrastructure; responsible for data quality and availability.
- Needs: clear quality reports, schema drift alerts, idempotent pipelines that can be safely re-run, and a simple CI/CD workflow.
- Success: zero silent pipeline failures; deploys new data sources in under a day.

**Retail Manager / Exec — "Carlos"**
- Reads dashboards; never touches data tooling directly.
- Needs: reliable KPI numbers with visible freshness timestamps; trend indicators.
- Success: opens the dashboard and trusts the numbers on sight.

### 4.2 Real Audience (Portfolio)

Technical hiring managers, data platform leads, and collaborators who will evaluate this repository to assess expertise in:

- **Data Engineering** — pipeline design, Delta Lake, idempotency, incremental loads.
- **Data Modeling** — Snowflake Schema, SCD Type 2, Star Schema, OBT.
- **Data Quality** — expectation-based validation, severity levels, quality logging.
- **DataOps** — testing, CI/CD, environment promotion, IaC with Databricks Asset Bundles.
- **FinOps** — cost-aware architecture choices (partitioning, single-node Free Tier, compute sizing).
- **AI / LLM** — agentic Text-to-SQL and natural language insights.

---

## 5. Feature Requirements

Features are prioritized as **P0** (must-have, core value), **P1** (important, differentiating), or **P2** (nice-to-have, stretch).

Status: **Shipped** | **In Progress** | **Planned**

---

### 5.1 Data Ingestion — Bronze Layer

| # | Requirement | Priority | Status |
|---|---|---|---|
| B1 | Ingest CSV/JSON/Parquet files from the Landing Zone as-is, preserving raw history. | P0 | Shipped |
| B2 | Validate incoming files against JSON Schema contracts; halt on breaking schema drift. | P0 | Shipped |
| B3 | Warn (do not halt) on additive schema drift (new columns not in the contract). | P1 | Shipped |
| B4 | Append Bronze metadata columns: `_source`, `_source_file`, `_batch_id`, `_ingested_at`, `ingestion_date`. | P0 | Shipped |
| B5 | Idempotent upsert — merging by `(primary_key, _source_file)` prevents duplicate rows on re-run. | P0 | Shipped |
| B6 | Partition Bronze tables by `ingestion_date` for pruning efficiency. | P1 | Shipped |
| B7 | Log pipeline run metadata (run_id, rows_read, rows_written, duration, status). | P1 | Shipped |
| B8 | Data quality checks on Bronze output with CRITICAL / WARNING severity levels. | P0 | Shipped |

---

### 5.2 Data Transformation — Silver Layer

| # | Requirement | Priority | Status |
|---|---|---|---|
| S1 | Model data as a **Snowflake Schema**: `fact_sales`, `dim_customer`, `dim_product`, `dim_store`, `dim_date`, `dim_inventory`. | P0 | Shipped |
| S2 | Apply **SCD Type 2** on `dim_customer` and `dim_product` — track historical changes with `valid_from`, `valid_to`, `is_current`. | P0 | Shipped |
| S3 | Cleanse and conform data types; standardise nulls and categorical values. | P0 | Shipped |
| S4 | Data quality checks on Silver output (null checks, referential integrity, SCD invariants). | P0 | Shipped |

---

### 5.3 Data Serving — Gold Layer

| # | Requirement | Priority | Status |
|---|---|---|---|
| G1 | Build a **Star Schema** optimised for BI tools: `fact_sales` joined to all dimensions. | P0 | Shipped |
| G2 | Build an **OBT (One Big Table)** for self-service analysts: `obt_sales`. | P1 | Shipped |
| G3 | Build **KPI aggregation tables**: `agg_daily_sales`, `agg_monthly_sales`. | P0 | Shipped |
| G4 | Data quality checks on Gold output (revenue totals, row count thresholds). | P0 | Shipped |

---

### 5.4 Orchestration

| # | Requirement | Priority | Status |
|---|---|---|---|
| O1 | **Databricks Lakeflow** job definition that runs Bronze → Silver → Gold in sequence with dependency ordering. | P1 | Planned |
| O2 | **Apache Airflow DAG** for the full pipeline, enabling cross-platform and external-dependency scheduling. | P1 | Planned |
| O3 | Airflow DAG for data quality report generation. | P2 | Planned |

---

### 5.5 Observability — Data Quality Dashboard

| # | Requirement | Priority | Status |
|---|---|---|---|
| DQ1 | Streamlit app showing pipeline run history: run_id, layer, status, rows written, duration. | P0 | Planned |
| DQ2 | Quality check results per run: rule name, severity, pass/fail, affected rows. | P0 | Planned |
| DQ3 | Trend charts for quality failure rate over time, per layer. | P1 | Planned |
| DQ4 | Alerting on CRITICAL quality failures (email or Slack webhook). | P2 | Planned |

---

### 5.6 Business Intelligence Dashboard

| # | Requirement | Priority | Status |
|---|---|---|---|
| BI1 | Streamlit app with retail KPIs: total revenue, orders, average basket size, top products, top stores. | P0 | Planned |
| BI2 | Time-series charts for sales trends (daily, monthly, year-over-year). | P0 | Planned |
| BI3 | Inventory health view: stock levels by product and store, low-stock alerts. | P1 | Planned |
| BI4 | Customer behaviour view: new vs. returning customers, cohort retention. | P1 | Planned |
| BI5 | Filterable by date range, store, product category. | P0 | Planned |
| BI6 | Data freshness timestamp visible on every dashboard page. | P1 | Planned |

---

### 5.7 Agentic AI — Text-to-SQL and Natural Language Insights

| # | Requirement | Priority | Status |
|---|---|---|---|
| AI1 | Conversational Streamlit interface where users ask questions in plain English. | P0 | Planned |
| AI2 | LLM-generated SQL queries against the Gold layer, with query results displayed. | P0 | Planned |
| AI3 | Natural language summary of query results ("Sales were up 12% last week, driven by Store 3"). | P1 | Planned |
| AI4 | Query history with the ability to re-run or refine past questions. | P2 | Planned |
| AI5 | Guard-rails: read-only access; reject queries outside the retail data domain. | P0 | Planned |
| AI6 | Powered by Claude API (`claude-sonnet-4-6`) via the Anthropic SDK. | P0 | Planned |

---

### 5.8 CI/CD and DataOps

| # | Requirement | Priority | Status |
|---|---|---|---|
| CD1 | GitHub Actions workflow: lint (ruff), unit tests, coverage gate (≥80%) on every pull request. | P0 | Planned |
| CD2 | GitHub Actions workflow: deploy to DEV on merge to `main`. | P0 | Planned |
| CD3 | GitHub Actions workflow: promote DEV → UAT → PROD via manual approval gates. | P1 | Planned |
| CD4 | **Databricks Asset Bundles (DAB)** configuration for DEV, UAT, and PROD targets. | P1 | Planned |
| CD5 | Integration tests that run the full pipeline end-to-end in a CI environment. | P1 | Planned |

---

### 5.9 Deployment

| # | Requirement | Priority | Status |
|---|---|---|---|
| DEP1 | Local mode: run the full pipeline on a laptop with no cloud dependency. | P0 | Shipped |
| DEP2 | Databricks Free Tier: one-notebook deployment via Databricks Repos. | P0 | Shipped |
| DEP3 | Databricks on Azure with ADLS Gen2 storage and Service Principal auth. | P1 | Planned |
| DEP4 | Databricks on AWS with S3 and IAM instance profiles. | P2 | Planned |
| DEP5 | Databricks on GCP with GCS and Service Account key. | P2 | Planned |

---

## 6. Data Sources

| Source | Format | Ingestion Frequency | Primary Key |
|---|---|---|---|
| Sales transactions | CSV | Daily | `transaction_id` |
| Customer master | CSV | Daily (SCD2) | `customer_id` |
| Product catalog | CSV | Daily (SCD2) | `product_id` |
| Store master | CSV | On change | `store_id` |
| Inventory snapshots | CSV | Daily | `inventory_id` |

---

## 7. Non-Functional Requirements

| Category | Requirement |
|---|---|
| **Idempotency** | Re-running any pipeline layer with the same input must produce the same output. |
| **Observability** | Every pipeline run produces a structured log entry: run_id, layer, status, rows, duration. |
| **Quality enforcement** | CRITICAL quality failures halt the pipeline immediately; WARNING failures are logged and tolerated. |
| **Schema safety** | Breaking schema changes (dropped required columns) halt Bronze ingestion; additive drift is logged only. |
| **Portability** | All storage paths are injected via environment variables — no hardcoded cloud paths. |
| **Test coverage** | Unit test coverage ≥ 80% enforced by CI gate. |
| **Cost awareness** | Partitioning strategy and single-node cluster choice on Free Tier reflect FinOps discipline. |

---

## 8. Success Metrics

| Metric | Target |
|---|---|
| Pipeline reliability | Zero silent data losses — all failures are surfaced in the quality log. |
| Data freshness | Gold layer updated within 1 hour of Landing Zone file arrival (when orchestrated). |
| Quality coverage | 100% of Gold tables have at least one CRITICAL quality check. |
| Test coverage | ≥ 80% unit test coverage on all pipeline modules. |
| Self-service rate | Business Analyst persona can answer a new KPI question in ≤ 5 minutes using the BI dashboard or AI agent. |
| Portfolio signal | All six disciplines (Data Eng, Analysis, Science, Governance, FinOps, DataOps) have at least one shipped artefact. |

---

## 9. Milestones

| Milestone | Scope | Status |
|---|---|---|
| **M1 — Data Foundation** | Landing data, Bronze/Silver/Gold pipelines, quality checks, unit tests, local + Free Tier deployment | **Shipped** |
| **M2 — Orchestration** | Databricks Lakeflow job, Airflow DAGs | Planned |
| **M3 — Observability** | Data Quality Dashboard (Streamlit) | Planned |
| **M4 — Business Intelligence** | BI KPI Dashboard (Streamlit) | Planned |
| **M5 — Agentic AI** | Text-to-SQL agent (Claude API) | Planned |
| **M6 — DataOps** | GitHub Actions CI/CD, Databricks Asset Bundles, DEV → UAT → PROD promotion | Planned |
| **M7 — Multi-cloud** | Azure, AWS, GCP deployment guides + tested DAB targets | Planned |

---

## 10. Open Questions

| # | Question | Owner |
|---|---|---|
| OQ1 | Should the AI agent support multi-turn conversation, or single-shot Q&A only in the first version? | Rogerio |
| OQ2 | Which Airflow version and deployment target (local Docker, MWAA, Cloud Composer)? | Rogerio |
| OQ3 | Should the BI dashboard read directly from DBFS Delta tables, or via a SQL warehouse / JDBC endpoint? | Rogerio |
| OQ4 | Is Unity Catalog in scope for the Azure deployment (M7), or stay with legacy metastore? | Rogerio |
