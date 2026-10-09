# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Bronze: Raw Ingestion
# MAGIC Incrementally loads new CSV files with Auto Loader. No cleaning here: bronze is the raw record of what arrived.

# COMMAND ----------

# MAGIC %run ./config/notebook_config

# COMMAND ----------

%run ./config/notebook_config

from pyspark.sql import functions as F

bronze_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/bronze_schema")
    .load(raw_path)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

(
    bronze_stream.writeStream
    .option("checkpointLocation", f"{checkpoint_path}/bronze")
    .trigger(availableNow=True)      # process available files, then stop
    .toTable("bronze_sales")
    .awaitTermination()
)

# COMMAND ----------

display(spark.table("bronze_sales").limit(10))
print("Bronze rows:", spark.table("bronze_sales").count())
