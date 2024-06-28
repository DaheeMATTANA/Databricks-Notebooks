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

countries.dtypes

# COMMAND ----------

countries_dt = spark.read.csv(path = countries_path, header = True)

# COMMAND ----------

countries_dt.dtypes

# COMMAND ----------

countries_dt.select(countries_dt['population'].cast(IntegerType())).dtypes

# COMMAND ----------

countries.select(countries['population'].cast(StringType())).dtypes

# COMMAND ----------

