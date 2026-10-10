# Databricks notebook source
# MAGIC %md
# MAGIC # Sales ETL Solution Accelerator
# MAGIC
# MAGIC ## Business problem
# MAGIC Order data arrives as daily extracts from source systems. The files contain duplicates,
# MAGIC missing values, and invalid records, so reports built directly on them are unreliable.
# MAGIC This accelerator consolidates the extracts into trusted, analytics-ready tables.
# MAGIC
# MAGIC ## What you get
# MAGIC | Layer | Table(s) | Purpose |
# MAGIC |---|---|---|
# MAGIC | **Bronze** | `bronze_sales` | Raw data exactly as received, plus ingestion metadata |
# MAGIC | **Silver** | `silver_sales`, `silver_sales_rejected` | Typed, validated, deduplicated data; bad rows are quarantined, not dropped |
# MAGIC | **Gold** | `gold_daily_sales`, `gold_sales_by_product_region` | Business aggregates for dashboards |
# MAGIC
# MAGIC ## Architecture
# MAGIC ```
# MAGIC  CSV files ──► Bronze ──► Silver ──► Gold ──► Dashboards
# MAGIC  (Volume)     (raw)      (clean)    (agg)
# MAGIC                            │
# MAGIC                            └──► Rejected (quarantine)
# MAGIC ```
# MAGIC
# MAGIC ## How to run
# MAGIC Run the notebooks in this order, on serverless compute or DBR 15.4 LTS or later:
# MAGIC 1. `01_Generate_Data`: creates synthetic CSV files in a Unity Catalog Volume
# MAGIC 2. `02_Bronze_Ingest`: loads the files with Auto Loader
# MAGIC 3. `03_Silver_Transform`: cleans, validates, and deduplicates
# MAGIC 4. `04_Gold_Aggregates`: builds the reporting tables
# MAGIC 5. `05_Validate`: asserts that the output is correct
# MAGIC 6. `99_Cleanup`: removes everything this accelerator created
# MAGIC
# MAGIC Or deploy everything as a workflow: `databricks bundle deploy -t dev` then
# MAGIC `databricks bundle run sales_etl_pipeline -t dev`.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Unity Catalog enabled
# MAGIC - Permission to `CREATE SCHEMA` and `CREATE VOLUME` in the catalog set in `config/notebook_config`
# MAGIC
# MAGIC ## Adapting it to your data
# MAGIC Only change `config/notebook_config` (catalog and paths) and replace `01_Generate_Data`
# MAGIC with your real source. Everything downstream works unchanged if your columns match,
# MAGIC otherwise adjust the column list in `03_Silver_Transform`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment check
# MAGIC Run the cell below to confirm your workspace is ready before starting.

# COMMAND ----------

# MAGIC %run ./config/notebook_config

# COMMAND ----------


print(f"User:     {username}")
print(f"Catalog:  {catalog}")
print(f"Schema:   {schema}")
print(f"Raw path: {raw_path}")

# Confirm we can write to the target schema
spark.sql(f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}._intro_check (id INT)")
spark.sql(f"DROP TABLE {catalog}.{schema}._intro_check")
print("Environment check passed: you can create tables in this schema.")
