# Databricks notebook source
# MAGIC %md
# MAGIC ## Reading Data

# COMMAND ----------

# MAGIC %md
# MAGIC ###### CSV

# COMMAND ----------

spark.read.csv('/FileStore/tables/countries.csv')

# COMMAND ----------

acountries_df = spark.read.csv('/FileStore/tables/countries.csv')

# COMMAND ----------

type(countries_df)

# COMMAND ----------

countries_df.show()

# COMMAND ----------

display(countries_df)

# COMMAND ----------

# Fix the header
countries_df = spark.read.csv('/FileStore/tables/countries.csv', header = True)

# COMMAND ----------

display(countries_df)

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

countries_df.schema

# COMMAND ----------

countries_df.describe()

# COMMAND ----------

countries_df = spark.read.options(header = True, inferSchema = True).csv('/FileStore/tables/countries.csv')
# Not very efficient (inferSchema)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Defining Schema

# COMMAND ----------

# Best way would be :
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

countries_df = spark.read.csv('/FileStore/tables/countries.csv', header = True, schema = countries_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Json

# COMMAND ----------

countries_sl_json = spark.read.json('/FileStore/tables/countries_single_line.json')

# COMMAND ----------

display(countries_sl_json)

# COMMAND ----------

countries_ml_json = spark.read.options(multiLine = True).json('/FileStore/tables/countries_multi_line.json')

# COMMAND ----------

display(countries_ml_json)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Tab-Delimited Text File

# COMMAND ----------

countries_txt = spark.read.csv('/FileStore/tables/countries.txt', header = True, sep = '\t')

# COMMAND ----------

display(countries_txt)