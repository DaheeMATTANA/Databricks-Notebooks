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

# MAGIC %md
# MAGIC ###### Filtering by population (> 1 bilion)

# COMMAND ----------

countries.filter(countries['population']>1000000000).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Filtering : capital city beginning with B

# COMMAND ----------

from pyspark.sql.functions import locate
countries.filter(locate('B', countries['capital']) == 1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### SQL syntax

# COMMAND ----------

countries.filter('region_id == 10').display()

# COMMAND ----------

countries.filter('region_id != 10').display()

# COMMAND ----------

countries.filter('region_id == 10 and population == 0').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Country name > 15 characters, region_id NOT 10

# COMMAND ----------

from pyspark.sql.functions import length

# COMMAND ----------

countries.filter(  (length(countries['name']) > 15) & (countries['region_id'] != 10)  ).display()

# COMMAND ----------

countries.filter('length(name) > 15 and region_id != 10').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conditional Statements

# COMMAND ----------

from pyspark.sql.functions import when

countries.withColumn('name_length', when(countries['population'] > 100000000, 'large').when(countries['population'] <= 100000000, 'not large')).display()

# COMMAND ----------

countries.withColumn('name_length', when(countries['population'] > 100000000, 'large').otherwise('not large')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expr : allows to use SQL functions

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

countries.select(expr('NAME as country_name')).display()

# COMMAND ----------

countries.select(expr('left(NAME, 2) as name')).display()

# COMMAND ----------

countries.withColumn('population_class', expr("CASE WHEN population > 100000000 THEN 'large' WHEN population > 50000000 THEN 'medium' ELSE 'small' END")).display()

# COMMAND ----------

countries.withColumn('area_class', expr("CASE WHEN area_km2 > 1000000 THEN 'large' WHEN area_km2 > 300000 THEN 'medium' ELSE 'small' END")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Removing Columns

# COMMAND ----------

countries_2 = countries.select('name', 'capital', 'population')

# COMMAND ----------

countries_2.display()

# COMMAND ----------

countries_3 = countries.drop(countries['organization_region_id'])

# COMMAND ----------

countries_3.display()

# COMMAND ----------

countries_3.drop('sub_region_id', 'intermediate_region_id').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grouping

# COMMAND ----------

from pyspark.sql.functions import *
countries. \
groupBy('region_id'). \
sum('population'). \
display()

# COMMAND ----------

countries. \
groupBy('region_id'). \
avg('population'). \
display()

# COMMAND ----------

countries.groupBy('region_id').sum('population', 'area_km2').display()

# COMMAND ----------

countries.groupBy('region_id').agg(avg('population'), sum('area_km2')).display()

# COMMAND ----------

countries.groupBy('region_id', 'sub_region_id').agg(avg('population'), sum('area_km2')).display()

# COMMAND ----------

countries.groupBy('region_id', 'sub_region_id'). \
agg(avg('population'), sum('area_km2')). \
withColumnRenamed('avg(population)', 'avg_pop').\
withColumnRenamed('sum(area_km2)', 'total_area').\
display()

# COMMAND ----------

countries.groupBy('region_id', 'sub_region_id'). \
agg(avg('population').alias('avg_pop'), sum('area_km2').alias('total_area')). \
display()

# COMMAND ----------

countries. \
groupBy('region_id', 'sub_region_id'). \
agg(min('population').alias('min_pop'), max('population').alias('max_pop')). \
sort(countries['region_id'].asc()). \
display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pivotting Dataframes

# COMMAND ----------

countries.groupBy('sub_region_id').pivot('region_id').sum('population').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joining Dataframes

# COMMAND ----------

regions_path = 'dbfs:/FileStore/tables/country_regions.csv'

regions_schema = StructType([
                    StructField('Id', StringType(), False),
                    StructField('NAME', StringType(), False)
                ]
                )

regions = spark.read.csv(path = regions_path, header = True, schema = regions_schema)

# COMMAND ----------

countries.join(regions, countries['region_id'] == regions['Id'], 'inner').display()

# COMMAND ----------

countries.join(regions, countries['region_id'] == regions['Id'], 'right').select(countries['name'], regions['name'], countries['population']).display()

# COMMAND ----------

countries. \
join(regions, countries['region_id'] == regions['Id'], 'inner'). \
select(regions['name'].alias('region_name'), countries['name'].alias('country_name'), countries['population']). \
sort(countries['population'].desc()). \
display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union

# COMMAND ----------

countries.count()

# COMMAND ----------

countries.union(countries).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unpivot

# COMMAND ----------

countries_joined = countries.join(regions, countries['region_id'] == regions['Id'], 'inner'). \
select(countries['name'].alias('country_name'), regions['name'].alias('region_name'), countries['population'])

# COMMAND ----------

pivot_countries = countries_joined.groupBy('country_name').pivot('region_name').sum('population')
pivot_countries.display()

# COMMAND ----------

pivot_countries.select('country_name', expr("stack(5, 'Africa', Africa, 'America', America, 'Asia', Asia, 'Europe', Europe, 'Oceania', Oceania) AS (region_name, population)")).filter('population IS NOT NULL').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pandas

# COMMAND ----------

# spark df to pandas df
import pandas as pd

# COMMAND ----------

countries_pd = countries.toPandas()

# COMMAND ----------

countries_pd.head()

# COMMAND ----------

countries_pd.iloc[0]

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/bronze')

# COMMAND ----------

