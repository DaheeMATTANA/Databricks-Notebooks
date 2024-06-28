# Databricks notebook source
# MAGIC %md
# MAGIC ## Accessing Data via Access Keys

# COMMAND ----------

countries = spark.read.csv("abfss://bronze@datalake1351831800.dfs.core.windows.net/countries.csv", header = True)

# COMMAND ----------

regions = spark.read.csv("abfss://bronze@datalake1351831800.dfs.core.windows.net/country_regions.csv", header = True)

# COMMAND ----------

regions.display()

# COMMAND ----------

countries.display()

# COMMAND ----------

regions = spark.read.csv("abfss://bronze@datalake1351831800.dfs.core.windows.net/country_regions.csv", header = True)

# COMMAND ----------

regions.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Via SAS Token

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mounting ADLS to DBFS

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs
# MAGIC ls
