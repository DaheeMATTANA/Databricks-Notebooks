# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.run('Worker Notebook', 60)

# COMMAND ----------

# in case error in a line of code
try:
    dbutils.notebook.run('Worker Notebook', 60)
except:
    print('Error Occurred')

# COMMAND ----------

# MAGIC %md
# MAGIC # Text widgets

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.notebook.run('Worker Notebook', 60, {'input_widget' : 'From Master Notebook'})

# COMMAND ----------

