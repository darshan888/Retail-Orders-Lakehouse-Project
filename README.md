# Retail-Orders-Lakehouse-Project

This project is a complete AWS analytics pipeline from scratch using a medallion architecture:
- `Bronze`: raw data landed in Amazon S3 and converted to Parquet by AWS Glue
- `Silver`: cleansed and conformed S3 datasets built by AWS Glue
- `Gold`: analytics marts materialized in Amazon Redshift with SQL
- `BI`: Amazon QuickSight dashboards on top of Redshift views


Solution and Approach :

1. Medallion Architecture — Organized data into Bronze (raw Parquet), Silver (cleaned & conformed), and Gold (Redshift analytics marts) layers using Amazon S3 and Parquet for cost-efficient, query-optimized storage.
2. Batch Ingestion — Raw CSV files are landed in S3 and converted to immutable Parquet format with ingestion metadata via bronze_ingest.py, a Glue PySpark job.
3. Automated ETL — Built 2 Glue PySpark jobs (bronze_ingest.py, silver_transform.py) for idempotent transformations — standardizing types, deduplicating records — with Glue Crawlers maintaining schema across Bronze and Silver layers in the Glue Data Catalog.
4. Orchestration — Used AWS Glue Workflow and Triggers to coordinate crawlers and ETL jobs in a sequential pipeline, ensuring end-to-end lineage from landing to gold.
5. Gold Layer Materialization — Connected Silver layer to Amazon Redshift Serverless via Redshift Spectrum external schemas, materializing dimensional and aggregate analytics models as gold marts using SQL.
6. Serving & Visualization — Built Amazon QuickSight dashboards on top of Redshift gold views for business analytics - top products, customer segmentation, and order analysis.

