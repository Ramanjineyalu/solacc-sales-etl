# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Validate Pipeline Output

# COMMAND ----------

# MAGIC %run ./config/notebook_config

# COMMAND ----------

%run ./config/notebook_config

silver = spark.table("silver_sales")
rejected = spark.table("silver_sales_rejected")
gold = spark.table("gold_daily_sales")

assert silver.count() > 0, "Silver table is empty"
assert silver.filter("quantity <= 0 OR customer_id IS NULL").count() == 0, "Invalid rows leaked into silver"
assert silver.count() == silver.select("order_id").distinct().count(), "Duplicate order_ids in silver"
assert rejected.count() > 0, "Expected rejected rows from injected bad data"
assert gold.count() > 0, "Gold table is empty"

# Revenue reconciliation: gold must equal silver
silver_rev = silver.agg({"total_amount": "sum"}).first()[0]
gold_rev = gold.agg({"revenue": "sum"}).first()[0]
assert abs(silver_rev - gold_rev) < 1.0, f"Revenue mismatch: {silver_rev} vs {gold_rev}"

print("All validations passed")
