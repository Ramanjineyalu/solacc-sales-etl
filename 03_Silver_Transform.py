# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Silver: Clean & Validate
# MAGIC - Cast to proper types and standardize text
# MAGIC - Validate business rules; send failures to `silver_sales_rejected`
# MAGIC - Deduplicate on `order_id` (keep the latest ingested record)

# COMMAND ----------

# MAGIC %run ./config/notebook_config

# COMMAND ----------


from pyspark.sql import functions as F, Window

bronze = spark.table("bronze_sales")

typed = (
    bronze.select(
        F.col("order_id").cast("int").alias("order_id"),
        F.to_date("order_date").alias("order_date"),
        F.nullif(F.trim("customer_id"), F.lit("")).alias("customer_id"),
        F.upper(F.trim("product_id")).alias("product_id"),
        F.initcap(F.trim("product_name")).alias("product_name"),
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unit_price").cast("double").alias("unit_price"),
        F.initcap(F.trim("region")).alias("region"),
        "_ingested_at", "_source_file",
    )
    .withColumn("total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
)

# COMMAND ----------

# Business rules
is_valid = F.coalesce(
    F.col("order_id").isNotNull()
    & F.col("order_date").isNotNull()
    & F.col("customer_id").isNotNull()
    & (F.col("quantity") > 0)
    & (F.col("unit_price") > 0),
    F.lit(False),
)

valid = typed.filter(is_valid)
rejected = typed.filter(~is_valid)

# Deduplicate: keep the latest ingested row per order_id
w = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
deduped = (
    valid.withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .drop("_rn")
)

# COMMAND ----------

# Full recompute from bronze keeps this step idempotent
deduped.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_sales")
rejected.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_sales_rejected")

print("Silver rows:  ", spark.table("silver_sales").count())
print("Rejected rows:", spark.table("silver_sales_rejected").count())
display(spark.table("silver_sales_rejected"))
