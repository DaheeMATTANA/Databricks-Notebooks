# Databricks notebook source
application_id = dbutils.secrets.get(scope = 'databricks-secrets-1358', key = 'application-id')
tenant_id = dbutils.secrets.get(scope = 'databricks-secrets-1358', key = 'tenant-id')
secret = dbutils.secrets.get(scope = 'databricks-secrets-1358', key = 'secret')

# COMMAND ----------

container_name = 'delta-lake-demo'
mount_point = '/mnt/delta-lake-demo'
account_name = 'datalake1351831800'

# COMMAND ----------

# syntax for configs and mount methods
configs = {"fs.azure.account.auth.type": "OAuth", "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider", "fs.azure.account.oauth2.client.id": application_id, "fs.azure.account.oauth2.client.secret": secret, "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(source = f"abfss://{container_name}@{account_name}.dfs.core.windows.net/", mount_point = mount_point, extra_configs = configs)

# COMMAND ----------

# DBTITLE 1,Loading Country Data from CSV with Spark
countries = spark.read.csv('dbfs:/mnt/bronze/countries.csv', header = True, inferSchema = True)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.write.format('delta').save('dbfs:/mnt/delta-lake-demo/countries_delta')

# COMMAND ----------

countries.write.format('parquet').save('dbfs:/mnt/delta-lake-demo/countries_parquet')

# COMMAND ----------

spark.read.format('delta').load('dbfs:/mnt/delta-lake-demo/countries_delta').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Partitionned file

# COMMAND ----------

countries.write.format('delta').mode('overwrite').partitionBy('region_id').save('dbfs:/mnt/delta-lake-demo/countries_delta_part')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE delta_lake_db;

# COMMAND ----------

countries = spark.read.format('delta').load('dbfs:/mnt/delta-lake-demo/countries_delta')

# COMMAND ----------

countries.write.saveAsTable('delta_lake_db.countries_managed_delta')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta_lake_db.countries_managed_delta

# COMMAND ----------

# for underlying data
countries.write.option('path', 'dbfs:/mnt/delta-lake-demo/countries_delta').mode('overwrite').saveAsTable('delta_lake_db.countries_ext_delta')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta_lake_db.countries_ext_delta

# COMMAND ----------

# MAGIC %md
# MAGIC # Deleting and Updating Records

# COMMAND ----------

