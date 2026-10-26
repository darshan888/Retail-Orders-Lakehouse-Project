# Retail-Orders-Lakehouse-Project

This project is a complete AWS analytics pipeline from scratch using a medallion architecture:
- `Bronze`: raw data landed in Amazon S3 and converted to Parquet by AWS Glue
- `Silver`: cleansed and conformed S3 datasets built by AWS Glue
- `Gold`: analytics marts materialized in Amazon Redshift with SQL
- `BI`: Amazon QuickSight dashboards on top of Redshift views
