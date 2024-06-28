# Databricks notebook source
order_details = spark.read.parquet('dbfs:/FileStore/tables/Gold/order_details')
monthly_sales = spark.read.parquet('dbfs:/FileStore/tables/Gold/monthly_sales')

# COMMAND ----------

display(order_details)

# COMMAND ----------

# MAGIC %md
# MAGIC some text for my notebook
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Title for my dashboard