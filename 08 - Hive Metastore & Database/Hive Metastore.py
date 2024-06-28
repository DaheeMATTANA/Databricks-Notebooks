# Databricks notebook source
countries = spark.read.csv('dbfs:/FileStore/tables/countries.csv', header = True)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.createTempView('countries_tv')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM countries_tv

# COMMAND ----------

spark.sql("SELECT * FROM countries_tv").display()

# COMMAND ----------

table_name = 'countries_tv'

# COMMAND ----------

spark.sql(f"SELECT * FROM {table_name}").display()

# COMMAND ----------

countries.createOrReplaceGlobalTempView('countries_gv')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.countries_gv

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS countires;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Managed Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC USE countries;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED countries

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE countries

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Managed Tables

# COMMAND ----------

countries = spark.read.csv('dbfs:/FileStore/tables/countries.csv', header = True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC USE default

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database()

# COMMAND ----------

countries.write.saveAsTable('countries.countries_mt')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED countries.countries_mt

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 2e methode : 1) create empty table
# MAGIC
# MAGIC CREATE TABLE countries.countries_mt_empty
# MAGIC   (
# MAGIC     country_id INT,
# MAGIC     name STRING,
# MAGIC     nationality STRING,
# MAGIC     country_code STRING,
# MAGIC     iso_alpha_2 STRING,
# MAGIC     capital STRING,
# MAGIC     population INT,
# MAGIC     area_km2 INT,
# MAGIC     region_id INT,
# MAGIC     sub_region_id INT,
# MAGIC     intermediate_region_id INT,
# MAGIC     organization_region_id INT
# MAGIC   )
# MAGIC USING CSV;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM countries.countries_mt_empty

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2e methode : 2) create a table using the data from countries_mt table
# MAGIC
# MAGIC CREATE TABLE countries.countries_copy AS
# MAGIC SELECT *
# MAGIC FROM countries.countries_mt

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM countries.countries_copy

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED countries.countries_copy

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.write.option('path', 'dbfs:/FileStore/external/countries').saveAsTable('countries.countries_ext_python')

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED countries.countries_ext_python

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE countries.countries_ext_sql
# MAGIC   (
# MAGIC     country_id INT,
# MAGIC     name STRING,
# MAGIC     nationality STRING,
# MAGIC     country_code STRING,
# MAGIC     iso_alpha_2 STRING,
# MAGIC     capital STRING,
# MAGIC     population INT,
# MAGIC     area_km2 INT,
# MAGIC     region_id INT,
# MAGIC     sub_region_id INT,
# MAGIC     intermediate_region_id INT,
# MAGIC     organization_region_id INT
# MAGIC   )
# MAGIC USING CSV
# MAGIC LOCATION 'dbfs:/FileStore/tables/countries.csv';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permanent Views

# COMMAND ----------

countries.write.saveAsTable('countries.countries_mt')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM countries_mt

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW countries.view_region_10
# MAGIC AS SELECT * FROM countries.countries_mt
# MAGIC WHERE region_id = 10

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table countries_mt

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM countries.view_region_10

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE countries CASCADE

# COMMAND ----------

