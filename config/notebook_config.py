# Databricks notebook source
import re

username = spark.sql("SELECT current_user()").first()[0]
clean_user = re.sub(r"[^a-zA-Z0-9]", "_", username.split("@")[0])

# Change the catalog if "main" isn't writable in your workspace
catalog = "main"
schema = f"solacc_sales_etl_{clean_user}"
volume = "raw_data"

raw_path = f"/Volumes/{catalog}/{schema}/{volume}/sales"
checkpoint_path = f"/Volumes/{catalog}/{schema}/{volume}/_checkpoints"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
spark.sql(f"USE {catalog}.{schema}")
