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
                    
countries = spark.read.csv(path = countries_path, header = True, schema = countries_schema)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.select(countries['population']/1000000).withColumnRenamed('(population / 1000000)', 'population_m').display()

# COMMAND ----------

countries_2 = countries.select(countries['population']/1000000).withColumnRenamed('(population / 1000000)', 'population_m')

# COMMAND ----------

countries_2.display()

# COMMAND ----------

from pyspark.sql.functions import round
countries_2.select(round(countries_2['population_m'], 3)).display()

# COMMAND ----------

countries.withColumn('population_m', round((countries['population']/1000000), 1)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Sort Functions

# COMMAND ----------

from pyspark.sql.functions import asc

countries.sort(countries['population'].asc()).display()

# COMMAND ----------

countries.sort(countries['population'].desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### String Functions

# COMMAND ----------

from pyspark.sql.functions import *

countries.select(upper(countries['name'])).display()

# COMMAND ----------

countries.select(initcap(countries['name'])).display()

# COMMAND ----------

countries.select(length(countries['name'])).display()

# COMMAND ----------

countries.select(concat_ws(' ', countries['name'], countries['country_code'])).display()