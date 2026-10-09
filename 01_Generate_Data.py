# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Generate Synthetic Sales Data
# MAGIC Creates 3 CSV batches simulating daily order extracts from a source system,
# MAGIC including some intentionally bad records.

# COMMAND ----------

# MAGIC %run ./config/notebook_config

# COMMAND ----------

%run ./config/notebook_config


import csv, os, random
from datetime import date, timedelta

random.seed(42)
os.makedirs(raw_path, exist_ok=True)

products = {
    "P100": ("Laptop", 1200.00),
    "P200": ("Monitor", 300.00),
    "P300": ("Keyboard", 80.00),
    "P400": ("Mouse", 40.00),
    "P500": ("Headset", 150.00),
}
regions = ["north", "south", "east", "west"]
header = ["order_id", "order_date", "customer_id", "product_id",
          "product_name", "quantity", "unit_price", "region"]

def make_row(order_id, day):
    pid = random.choice(list(products))
    name, price = products[pid]
    return [order_id, day.isoformat(), f"C{random.randint(1, 200):04d}",
            pid, name, random.randint(1, 5), price, random.choice(regions)]

order_id = 1000
start = date(2025, 1, 1)

for batch in range(1, 4):
    rows = []
    for d in range(10):
        day = start + timedelta(days=(batch - 1) * 10 + d)
        for _ in range(random.randint(15, 25)):
            rows.append(make_row(order_id, day))
            order_id += 1

    # Inject dirty data
    rows.append(rows[0][:])                                  # duplicate order
    bad_qty = make_row(order_id, start); bad_qty[5] = -2     # negative quantity
    order_id += 1; rows.append(bad_qty)
    no_cust = make_row(order_id, start); no_cust[2] = ""     # missing customer
    order_id += 1; rows.append(no_cust)

    with open(f"{raw_path}/sales_batch_{batch}.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

print(f"Wrote files to {raw_path}")
display(dbutils.fs.ls(raw_path))
