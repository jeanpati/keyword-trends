# Keyword Trends Insights Platform

A unified analytics platform for uncovering trends and patterns in online content consumption and search behavior. This project integrates Google Trends, YouTube API, and U.S. Census data into a centralized, reliable analytics environment, providing actionable insights for marketers, content creators, researchers, and data-driven organizations.

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

The **Keyword Trends Insights Platform** provides a centralized analytics solution for understanding digital interest across different demographics and geographies. By combining search data, video engagement metrics, and demographic/economic context, this platform enables data-driven decision-making and trend identification.

Key objectives:

- Consolidate disparate datasets into a single source of truth.
- Provide robust data transformation and quality processes.
- Enable analytics and visualization for stakeholders.
- Support scalable and maintainable workflows using modern open-source technologies.

---

## Key Features

- Automated ingestion of Google Trends, YouTube API, and U.S. Census data
- Raw data storage in MinIO as JSON, CSV, and Parquet files
- Modular data transformation with dbt, including Bronze, Silver, and Gold layers
- Scalable, query-optimized storage and analytics using DuckDB and DuckLake
- Orchestrated pipeline with Prefect or Airflow for scheduling and dependency management
- Interactive dashboards via Metabase for business users
- API-ready architecture for programmatic access and downstream applications

---

## Data Sources

| Source               | Description                                                         | Frequency        |
| -------------------- | ------------------------------------------------------------------- | ---------------- |
| **Google Trends**    | Search interest by keyword, topic, and U.S. region                  | Daily/Weekly     |
| **YouTube API**      | Video metadata including views, likes, comments, and trending stats | Daily            |
| **U.S. Census Data** | Demographic and economic metrics by state                           | Quarterly/Annual |

---

## Technical Architecture

The platform is built for scalability, modularity, and maintainability. It separates concerns across ingestion, storage, transformation, and visualization.

| Component                 | Role                        | Purpose                                                                                            |
| ------------------------- | --------------------------- | -------------------------------------------------------------------------------------------------- |
| **Custom Python Scripts** | Data Ingestion              | Fetch data from APIs and sources, transform minimally, and load raw files to MinIO                 |
| **MinIO**                 | Raw Data Lake               | Persistent storage for unprocessed JSON, CSV, and Parquet files                                    |
| **DuckDB / DuckLake**     | Analytical Engine & Storage | Efficient storage and query engine for transformation, aggregation, and analytics                  |
| **dbt**                   | Data Transformation         | ELT framework for converting raw files into Bronze, Silver, and Gold layers with automated testing |
| **Airflow / Prefect**     | Orchestration               | Schedules and manages end-to-end data workflows                                                    |
| **Metabase**              | BI & Visualization          | Interactive dashboards and reporting for analysts and business stakeholders                        |

---

## Project Workflow

### 1. Automated Ingestion

- Prefect or Airflow triggers ingestion scripts on a defined schedule.
- Scripts extract data from Google Trends, YouTube API, and Census datasets.
- Raw data is stored in MinIO in JSON, CSV, or Parquet format.

### 2. Bronze Layer Creation

- dbt models read raw files from MinIO and generate Parquet-based Bronze tables in DuckLake.
- Minimal transformations are applied: flattening nested JSON, type conversions, and ensuring queryability.
- Bronze tables act as the source-of-truth for all downstream processing.

### 3. Silver & Gold Layer Transformations

- Silver Layer: Cleaned and validated data, ready for analytics.
- Gold Layer: Business-ready tables with aggregated, joined, and enriched datasets for direct stakeholder consumption.
- Example: Merging YouTube search data with Google Trends and Census demographics to analyze keyword trends by geography and demographics.
- dbt tests ensure data quality at every stage.

### 4. Data Delivery & Visualization

- Metabase dashboards provide non-technical users with actionable insights.
- DuckDB/DuckLake allows direct querying for advanced analytics or custom applications.

---

## Analytical Layers

| Layer      | Description                                   | Purpose                                   |
| ---------- | --------------------------------------------- | ----------------------------------------- |
| **Bronze** | Raw, minimally processed data                 | Source-of-truth for all analytics         |
| **Silver** | Cleaned and validated datasets                | Reliable analytics-ready tables           |
| **Gold**   | Aggregated, enriched, and business-ready data | Dashboards, reports, and decision support |

---

## Actionable Insights

- **Businesses:** Evaluate potential market demand by tracking keyword search trends and video engagement metrics.
- **Marketers / Content Creators:** Identify emerging topics and trends for specific regions or demographics.
- **Researchers / Policy Analysts:** Correlate search behavior with demographic and economic factors to uncover public interest patterns and social trends.

---

## Getting Started

1. Clone the repository:

```bash
git clone https://github.com/your-username/keyword-trends-platform.git
cd keyword-trends-platform
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Configure environment variables for API keys, DuckLake paths, and MinIO credentials.

4. Initialize and run dbt models:

```bash
dbt deps
dbt run
dbt test
```

5. Launch Metabase and connect to DuckDB/DuckLake for dashboard visualization.

---

## Future Enhancements

- Integrate additional social media platforms (e.g., TikTok, Twitter) for trend analysis.

- Implement predictive analytics to forecast emerging trends.

- Add real-time streaming ingestion for YouTube trending videos.

- Enhance API endpoints for programmatic access by external applications.
