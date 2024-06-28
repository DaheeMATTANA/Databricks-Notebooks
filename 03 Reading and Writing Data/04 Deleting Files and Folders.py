# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/tables/countries.txt')

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/tables/countries_multi_line.json')

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/tables/countries_single_line.json')

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/tables/countries_out', recurse = True)
dbutils.fs.rm('dbfs:/FileStore/tables/output', recurse = True)

# COMMAND ----------

