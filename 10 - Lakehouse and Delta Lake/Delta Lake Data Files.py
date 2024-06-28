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

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta_lake_db.countries_managed_delta

# COMMAND ----------

countries = spark.read.csv('/mnt/bronze/countries.csv', header = True, inferSchema = True)

# COMMAND ----------

# Create managed parquet table
countries.write.format('parquet').saveAsTable('delta_lake_db.countries_managed_pq')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta_lake_db.countries_managed_pq

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %sql
# MAGIC USE delta_lake_db

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All records from the parquet managed table
# MAGIC SELECT *
# MAGIC FROM countries_managed_pq

# COMMAND ----------

# MAGIC %sql
# MAGIC -- All records from the delta table
# MAGIC SELECT *
# MAGIC FROM countries_managed_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This will not work
# MAGIC DELETE FROM countries_managed_pq
# MAGIC WHERE region_id = 20

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM countries_managed_delta
# MAGIC WHERE region_id = 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM countries_managed_delta

# COMMAND ----------

# Underlying data
spark.read.format('delta').load('dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_delta').display()

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, 'dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_delta')

# COMMAND ----------

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("region_id = 40 and population > 200000")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM countries_managed_delta
# MAGIC WHERE REGION_ID = 40 
# MAGIC AND POPULATION > 200000

# COMMAND ----------

# Declare the predicate by using Spark SQL functions.
deltaTable.delete(col('region_id') == 50)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Updating

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM countries_managed_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE countries_managed_delta
# MAGIC SET country_code = 'XXX'
# MAGIC WHERE region_id = 10

# COMMAND ----------

deltaTable.update(
    "region_id = 30 and area_km2 > 600000",
    {"country_code" : "'YYY'"}
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM countries_managed_delta
# MAGIC WHERE area_km2 > 60000

# COMMAND ----------

# MAGIC %md
# MAGIC # Merge into

# COMMAND ----------

countries = spark.read.csv('dbfs:/mnt/bronze/countries.csv', header = True, inferSchema = True)

# COMMAND ----------

countries.display()

# COMMAND ----------

# DF 1 : region_id = 10, 20, 30
countries_1 = countries.filter('region_id in (10, 20, 30)')

# COMMAND ----------

countries_1.display()

# COMMAND ----------

# DF 2 : region_id = 20, 30, 40, 50
countries_2 = countries.filter('region_id in (20, 30, 40, 50)')

# COMMAND ----------

countries_2.display()

# COMMAND ----------

countries_1.write.format('delta').saveAsTable('delta_lake_db.countries_1')

# COMMAND ----------

countries_2.write.format('delta').saveAsTable('delta_lake_db.countries_2')

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta_lake_db.countries_2
# MAGIC SET name = UPPER(name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM  delta_lake_db.countries_2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta_lake_db.countries_1
# MAGIC WHERE REGION_ID IN(40, 50)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO delta_lake_db.countries_1 tgt
# MAGIC USING delta_lake_db.countries_2 src
# MAGIC ON tgt.country_id = src.country_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.name = src.name
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     tgt.country_id,
# MAGIC     tgt.name,
# MAGIC     tgt.nationality,
# MAGIC     tgt.country_code,
# MAGIC     tgt.iso_alpha2,
# MAGIC     tgt.capital,
# MAGIC     tgt.population,
# MAGIC     tgt.area_km2,
# MAGIC     tgt.region_id,
# MAGIC     tgt.sub_region_id,
# MAGIC     tgt.intermediate_region_id,
# MAGIC     tgt.organization_region_id
# MAGIC   )
# MAGIC   VALUES(
# MAGIC     src.country_id,
# MAGIC     src.name,
# MAGIC     src.nationality,
# MAGIC     src.country_code,
# MAGIC     src.iso_alpha2,
# MAGIC     src.capital,
# MAGIC     src.population,
# MAGIC     src.area_km2,
# MAGIC     src.region_id,
# MAGIC     src.sub_region_id,
# MAGIC     src.intermediate_region_id,
# MAGIC     src.organization_region_id
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta_lake_db.countries_1
# MAGIC ORDER BY 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM delta_lake_db.countries_1
# MAGIC WHERE region_id = 10
# MAGIC ORDER BY 2

# COMMAND ----------

# Same operation In Python

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, 'dbfs:/user/hive/warehouse/delta_lake_db.db/countries_1')

# COMMAND ----------

deltaTable.alias("target").merge(
    countries_2.alias("source"),
    "target.country_id = source.country_id") \
    .whenMatchedUpdate(set = {
        "name" : "source.name"
    }) \
    .whenNotMatchedInsert(values = {
        "country_id" : "source.country_id",
        "name" : "source.name",
        "nationality" : "source.nationality",
        "country_code" : "source.country_code",
        "iso_alpha2" : "source.iso_alpha2",
        "capital" : "source.capital",
        "population" : "source.population",
        "area_km2" : "source.area_km2",
        "region_id" : "source.region_id",
        "sub_region_id" : "source.sub_region_id",
        "intermediate_region_id" : "source.intermediate_region_id",
        "organization_region_id" : "source.organization_region_id"
    })\
    .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table utility commands

# COMMAND ----------

# MAGIC %md
# MAGIC ##### History of delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta_lake_db.countries_1

# COMMAND ----------

# in Python
# from delta.tables import *

deltaTable = DeltaTable.forPath(spark, 'dbfs:/user/hive/warehouse/delta_lake_db.db/countries_1')

deltaTable.history().display()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Access version 1
# MAGIC SELECT *
# MAGIC FROM delta_lake_db.countries_1 VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Access version 3
# MAGIC SELECT *
# MAGIC FROM delta_lake_db.countries_1 TIMESTAMP AS OF '2024-06-28T09:44:48.000+00:00'

# COMMAND ----------

spark.read.format('delta').option('versionAsOf', 1).load('dbfs:/user/hive/warehouse/delta_lake_db.db/countries_1').display()

# COMMAND ----------

spark.read.format('delta').option('timestampAsOf', '2024-06-28T09:44:48.000+00:00').load('dbfs:/user/hive/warehouse/delta_lake_db.db/countries_1').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Convert a Parquet table to a Delta Table

# COMMAND ----------

deltaTable = DeltaTable.convertToDelta(spark, "parquet.`dbfs:/user/hive/warehouse/delta_lake_db.db/countries_managed_pq`")

# COMMAND ----------


