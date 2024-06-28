# Databricks notebook source
# MAGIC %md
# MAGIC ## Writing Data

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType
countries_schema = StructType([
                    StructField("COUNTRY_ID", IntegerType(), False),
                    StructField("NAME", StringType(), False),
                    StructField("NATIONALITY", StringType(), False),
                    StructField("COUNTRY_CODE", StringType(), False),
                    StructField("ISO_ALPHA2", StringType(), False),
                    StructField("CAPITAL", StringType(), False),
                    StructField("POPULATION", DoubleType(), False),
                    StructField("AREA_KM2", IntegerType(), False),
                    StructField("REGION_ID", IntegerType(), True),
                    StructField("SUB_REGION_ID", IntegerType(), True),
                    StructField("INTERMEDIATE_REGION_ID", IntegerType(), True),
                    StructField("ORGANIZATION_REGION_ID", IntegerType(), True)
                    ]
                    )

# COMMAND ----------

countries_df = spark.read.csv('/FileStore/tables/countries.csv/', header = True, schema = countries_schema)

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df.write.csv('/FileStore/tables/countries_out', header = True)

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/tables/countries_out', header = True)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Partitioning

# COMMAND ----------

df.write.options(header = True).mode('overwrite').partitionBy('REGION_ID').csv('dbfs:/FileStore/tables/countries_out')

# COMMAND ----------

df2 = spark.read.csv('dbfs:/FileStore/tables/countries_out', header = True)

# COMMAND ----------

# Access to a single partition
df2 = spark.read.csv('dbfs:/FileStore/tables/countries_out/REGION_ID=10', header = True)

# COMMAND ----------

display(df2)

# COMMAND ----------

