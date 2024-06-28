# Databricks notebook source
countries_path = '/FileStore/tables/countries.csv'
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

countries = spark.read.csv(path = countries_path, header = True, schema = countries_schema)

# COMMAND ----------

from pyspark.sql.functions import current_date
countries.withColumn('current_date', current_date()).display()

# COMMAND ----------

from pyspark.sql.functions import lit
countries.withColumn('updated_by', lit('MV')).display()

# COMMAND ----------

countries.withColumn('population_m', countries['population'] / 1000000).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Sort Functions

# COMMAND ----------

from pyspark.sql.functions import asc
countries.sort(countries['population'].asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### String Functions

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

countries.select(initcap(countries['name'])).display()

# COMMAND ----------

countries.select(upper(countries['name'])).display()

# COMMAND ----------

countries.select(length(countries['name'])).display()

# COMMAND ----------

countries.select(concat_ws(' ', countries['name'], countries['country_code'])).display()

# COMMAND ----------

countries.select(concat_ws('-', countries['name'], lower(countries['country_code']))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Datetime Functions

# COMMAND ----------

countries = countries.withColumn('timestamp', current_timestamp())

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.select(year('timestamp')).display()

# COMMAND ----------

countries = countries.withColumn('date_literal', lit('27-10-2020'))

# COMMAND ----------

countries.display()

# COMMAND ----------

countries = countries.withColumn('date', to_date(countries['date_literal'], 'dd-MM-yyyy'))

# COMMAND ----------

countries.dtypes

# COMMAND ----------

