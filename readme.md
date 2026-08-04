# Serverless Clickstream ETL Pipeline on AWS (Medallion Architecture)

[![AWS](https://img.shields.io/badge/AWS-Glue-orange)](https://aws.amazon.com/glue/)
[![Spark](https://img.shields.io/badge/PySpark-3.5-blue)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

## Overview
End-to-end data pipeline that transforms raw e‑commerce clickstream events into:
- **Analytics-ready data marts** (daily KPIs, product performance, funnel analysis)
- **ML feature store** (user-level features for churn & purchase propensity)
- **Business dashboards** (Power BI connected to Athena)

Built entirely on AWS with a medallion architecture (Bronze → Silver → Gold) using **PySpark on AWS Glue**.

*Raw data (Kaggle zip) → S3 staging → Glue Jobs → S3 medallion layers → Glue Data Catalog → Athena → Power BI*

## Tech Stack
- **Orchestration:** AWS Glue Workflows
- **Compute:** AWS Glue (PySpark 3.5, G.1X workers)
- **Storage:** Amazon S3 (Parquet, partitioned)
- **Catalog:** AWS Glue Data Catalog
- **Queries:** Amazon Athena
- **Visualization:** Power BI (via Athena connector)
- **ML:** User feature tables (ready for SageMaker / scikit‑learn)

## Medallion Layers
| Layer | Description |
|-------|-------------|
| **Bronze** | Raw CSV events + `ingestion_date`, converted to Parquet |
| **Silver** | Clean, typed, enriched with `category_level_1..4`, `brand` |
| **Gold** | Dimensional model (dim_date, dim_product, dim_user) + fact tables (daily_kpi, fact_product_daily, fact_hourly_traffic, fact_user) + ML features |

## Star Schema (Gold Layer)
- `dim_date`, `dim_product`, `dim_user` – dimensions with surrogate keys
- `daily_kpi`, `fact_product_daily`, `fact_hourly_traffic`, `fact_user` – fact tables
- Drill‑down enabled via `category_level_1 → category_level_2 → category_level_3 → category_level_4`

## Sample Queries (Athena)
```sql
-- Daily revenue trend
SELECT d.event_date, SUM(f.total_revenue) AS revenue
FROM gold_analytics.fact_product_daily f
JOIN gold_analytics.dim_date d ON f.date_key = d.date_key
GROUP BY d.event_date ORDER BY d.event_date;

-- Top 5 categories by views
SELECT p.category_level_1, SUM(f.views) AS total_views
FROM gold_analytics.fact_product_daily f
JOIN gold_analytics.dim_product p ON f.product_id = p.product_id
GROUP BY p.category_level_1 ORDER BY total_views DESC LIMIT 5;