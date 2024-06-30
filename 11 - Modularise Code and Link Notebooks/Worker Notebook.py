# Databricks notebook source
print('Printed from Worker Notebook')

# COMMAND ----------

dbutils.notebook.exit('Worker Notebook Executed Successfully')

# COMMAND ----------

dbutils.widgets.text('input_widget', '', 'provide an input')

# COMMAND ----------

dbutils.widgets.get('input_widget')