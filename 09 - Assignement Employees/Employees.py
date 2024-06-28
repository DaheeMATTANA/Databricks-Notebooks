# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Mount the employees container to DBFS

# COMMAND ----------

application_id = dbutils.secrets.get(scope = 'databricks-secrets-1358', key = 'application-id')
tenant_id = dbutils.secrets.get(scope = 'databricks-secrets-1358', key = 'tenant-id')
secret = dbutils.secrets.get(scope = 'databricks-secrets-1358', key = 'secret')

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# syntax for configs and mount methods
configs = {"fs.azure.account.auth.type": "OAuth", "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider", "fs.azure.account.oauth2.client.id": application_id, "fs.azure.account.oauth2.client.secret": secret, "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(source = "abfss://employee@datalake1351831800.dfs.core.windows.net/", mount_point = "/mnt/employee", extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Adding parquet files to the silver folder

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Employees Table

# COMMAND ----------

# Reading in the hire_date as a string to avoid formatting issues during the read operation
employees_path = '/mnt/employee/bronze/employees.csv'

employees_schema = StructType([
                    StructField("EMPLOYEE_ID", IntegerType(), False),
                    StructField("FIRST_NAME", StringType(), False),
                    StructField("LAST_NAME", StringType(), False),
                    StructField("EMAIL", StringType(), False),
                    StructField("PHONE_NUMBER", StringType(), False),
                    StructField("HIRE_DATE", StringType(), False),
                    StructField("JOB_ID", StringType(), False),
                    StructField("SALARY", IntegerType(), False),
                    StructField("MANAGER_ID", IntegerType(), True),
                    StructField("DEPARTMENT_ID", IntegerType(), False)
                    ]
                    )

employees=spark.read.csv(path = employees_path, header = True, schema = employees_schema)

# COMMAND ----------

employees.display()

# COMMAND ----------

from pyspark.sql.functions import to_date
employees = employees.select(
    "employee_id",
    "first_name",
    "last_name",
    to_date(employees['hire_date'], "MM/dd/yyyy").alias('hire_date'),
    'job_id',
    'salary',
    'manager_id',
    'department_id'
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Departments Table

# COMMAND ----------

dept_path = '/mnt/employee/bronze/departments.csv'

dept_schema = StructType([
                StructField('DEPARTMENT_ID', IntegerType(), False),
                StructField('DEPARTMENT_NAME', StringType(), False),
                StructField('MANAGER_ID', IntegerType(), False),
                StructField('LOCATION_ID', IntegerType(), False)
                ]
                )

dept = spark.read.csv(path = dept_path, header = True, schema = dept_schema)

# COMMAND ----------


dept.display()

# COMMAND ----------

# dropping unnecessary columns
dept = dept.drop('MANAGER_ID', 'LOCATION_ID')

# COMMAND ----------

dept.write.parquet('/mnt/employee/silver/departments', mode = 'overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Countries Table

# COMMAND ----------

countries_path = '/mnt/employee/bronze/countries.csv'

countries_schema = StructType([
                StructField('COUNTRY_ID', StringType(), False),
                StructField('COUNTRY_NAME', StringType(), False)
                ]
                )

countries = spark.read.csv(path = countries_path, header = True, schema = countries_schema)

# COMMAND ----------

countries.display()

# COMMAND ----------

countries.write.parquet('/mnt/employee/silver/countries', mode = 'overwrite')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Adding parquet files to the gold folder of the employee container

# COMMAND ----------

employees = spark.read.parquet('/mnt/employee/silver/employees')

# COMMAND ----------

employees = spark.read.parquet('/mnt/employee/silver/employees')

# COMMAND ----------

employees.display()

# COMMAND ----------

# Full name column
from pyspark.sql.functions import concat_ws
employees = employees.withColumn('FULL_NAME', concat_ws(' ', employees['FIRST_NAME'], employees['LAST_NAME']))

# COMMAND ----------

# Drop unnecessary columns
employees = employees.drop('FIRST_NAME', 'LAST_NAME', 'MANAGER_ID')

# COMMAND ----------

departments = spark.read.parquet('/mnt/employee/silver/departments')

# COMMAND ----------

# Join department name
employees = employees.join(departments, employees['department_id'] == departments['department_id'], 'left'). \
select(
    'EMPLOYEE_ID',
    'FULL_NAME',
    'HIRE_DATE',
    'JOB_ID',
    'SALARY',
    'DEPARTMENT_NAME'
)

# COMMAND ----------

employees.display()

# COMMAND ----------

employees.write.parquet('/mnt/employee/gold/employees')

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Creating the employees database and loading the gold layer table
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS employees

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE employees.employees(
# MAGIC   EMPLOYEE_ID INT,
# MAGIC   FULL_NAME STRING,
# MAGIC   HIRE_DATE DATE,
# MAGIC   JOB_ID STRING,
# MAGIC   SALARY INT,
# MAGIC   DEPARTMENT_NAME STRING
# MAGIC )
# MAGIC USING PARQUET
# MAGIC LOCATION '/mnt/employee/gold/employees'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM employees.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED employees.employees

# COMMAND ----------

