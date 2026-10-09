# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Gold: Business Aggregates
# MAGIC Analytics-ready tables for dashboards and reporting.

# COMMAND ----------

# MAGIC %run ./config/notebook_config

# COMMAND ----------

%run ./config/notebook_config

from pyspark.sql import functions as F

silver = spark.table("silver_sales")

# Daily sales summary
(
    silver.groupBy("order_date")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.countDistinct("order_id").alias("orders"),
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
    .orderBy("order_date")
    .write.mode("overwrite").saveAsTable("gold_daily_sales")
)

# Sales by product and region
(
    silver.groupBy("region", "product_id", "product_name")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("revenue"),
        F.sum("quantity").alias("units_sold"),
    )
    .write.mode("overwrite").saveAsTable("gold_sales_by_product_region")
)

# COMMAND ----------

display(spark.table("gold_daily_sales"))
display(spark.table("gold_sales_by_product_region").orderBy(F.desc("revenue")))
