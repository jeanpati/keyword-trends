# Keyword Trends Insights Platform

A unified analytics platform for analyzing beauty and cosmetics trends using search behavior, video engagement, and demographic data. This project integrates Google Trends, YouTube APIs, and U.S. Census data into a centralized, reliable analytics environment, providing actionable insights for marketers, content creators, and data-driven organizations.

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Key Features](#key-features)
3. [Data Sources](#data-sources)
4. [Technical Architecture](#technical-architecture)
5. [Project Workflow](#project-workflow)
6. [Analytical Layers](#analytical-layers)
7. [Actionable Insights](#actionable-insights)
8. [Getting Started](#getting-started)
9. [Future Enhancements](#future-enhancements)
10. [Contact](#contact)

---

## Project Overview

The **Keyword Trends Insights Platform** provides an end-to-end solution for understanding digital interest in beauty and cosmetics across regions and demographics. By combining keyword search trends, video engagement metrics, and demographic context, this platform enables **data-driven decision-making** and trend discovery.

**Key objectives:**

- Consolidate multiple datasets into a single, reliable source of truth.
- Transform raw data into clean, analysis-ready formats using a **layered ELT process**.
- Provide **interactive dashboards** for stakeholders to explore trends.
- Orchestrate ingestion and transformations with **Airflow** for scalable, repeatable workflows.

---

## Key Features

- Automated ingestion of **Google Trends**, **YouTube Search API**, **YouTube Video API**, and **U.S. Census data**.
- Raw data storage in **MinIO** as CSV and Parquet files.
- Python scripts handle **CSV → Parquet conversion**, parsing multiple Google Trends files, and managing API calls.
- Modular transformations with **dbt**:
  - **Bronze**: raw and minimally processed data
  - **Silver**: cleaned, validated, and partitioned tables
  - **Gold**: aggregated, enriched tables ready for analysis
- Scalable querying and analytics using **DuckDB / DuckLake**.
- Orchestration with **Airflow**: separate DAGs for ingestion, parsing, YouTube search, YouTube stats, Silver transforms, and Gold transforms.
- **Metabase dashboards** for visualization of trends, demographics, and engagement metrics.

---

## Data Sources

| Source                 | Description                                                | Frequency                   |
| ---------------------- | ---------------------------------------------------------- | --------------------------- |
| **Keywords CSV**       | 50 curated beauty/cosmetics keywords                       | One-time / updated manually |
| **Google Trends CSVs** | Search interest by keyword and state (50 files)            | Weekly                      |
| **YouTube Search API** | Video metadata: names, channels, top 20 videos per keyword | Weekly                      |
| **YouTube Video API**  | Video statistics: views, likes, comments per video         | Daily                       |
| **U.S. Census Data**   | Demographics and economic metrics by state                 | Annual                      |

---

## Technical Architecture

| Component             | Role                      | Purpose                                                                                |
| --------------------- | ------------------------- | -------------------------------------------------------------------------------------- |
| **Python Scripts**    | Ingestion & Processing    | Convert CSV → Parquet, parse Google Trends, fetch YouTube search results & video stats |
| **MinIO**             | Object Storage            | Centralized storage for raw and processed files                                        |
| **DuckDB / DuckLake** | Data Lake & Analytical DB | Storage for Bronze/Silver/Gold layers; optimized for OLAP queries                      |
| **dbt**               | ELT Transformation        | Bronze → Silver → Gold transformations with testing and data quality checks            |
| **Airflow**           | Orchestration             | Manage DAGs for ingestion, parsing, transformations, and daily/weekly updates          |
| **PostgreSQL**        | BI Layer Storage          | Stores Gold tables for Metabase dashboards                                             |
| **Metabase**          | BI & Visualization        | Build interactive dashboards for trend analysis                                        |

---

## Project Workflow

1. **Ingestion**

   - Drop raw CSVs (keywords, Google Trends, Census) into **MinIO**.
   - Python scripts convert CSV → Parquet and save to DuckLake.
   - Google Trends CSVs parsed and loaded per keyword.

2. **YouTube Data Enrichment**

   - Read keywords from DuckLake.
   - Call **YouTube Search API** to fetch top 20 videos per keyword.
   - Call **YouTube Video API** daily for views, likes, comments, and push results to DuckLake.

3. **Transformation**

   - **Bronze layer**: raw, minimally processed tables.
   - **Silver layer**: cleaned, partitioned, validated tables.
   - **Gold layer**: aggregated, enriched tables ready for dashboards.
   - dbt ensures **data quality** and type consistency.

4. **Visualization**
   - Gold tables copied to **PostgreSQL**.
   - Metabase dashboards provide insights across keywords, demographics, and regions.

---

## Analytical Layers

| Layer      | Description                                 | Purpose                                       |
| ---------- | ------------------------------------------- | --------------------------------------------- |
| **Bronze** | Raw, minimally processed data               | Source-of-truth for all downstream processing |
| **Silver** | Cleaned and validated datasets              | Reliable analytics-ready tables               |
| **Gold**   | Aggregated, enriched, business-ready tables | Dashboards and decision-making insights       |

---

## Actionable Insights

- **Marketers & Content Creators**: Identify emerging beauty trends by region and demographics.
- **Businesses**: Track keyword popularity, YouTube engagement, and potential market demand.
- **Researchers**: Analyze correlations between demographics and content interest patterns.
- **Trend Forecasting**: Use daily video stats to detect rapid changes in content popularity.

---

## Getting Started

1. Clone the repository:

```bash
git clone https://github.com/your-username/keyword-trends-platform.git
cd keyword-trends-platform
```
