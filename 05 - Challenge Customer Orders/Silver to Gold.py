# Databricks notebook source
# MAGIC %md
# MAGIC ## Order Details Table

# COMMAND ----------

# DBTITLE 1,Load Parquet Orders Data into Spark
orders = spark.read.parquet('dbfs:/FileStore/tables/Silver/orders')
order_items = spark.read.parquet('dbfs:/FileStore/tables/Silver/order_items')
products = spark.read.parquet('dbfs:/FileStore/tables/Silver/products')
customers = spark.read.parquet('dbfs:/FileStore/tables/Silver/customers')

# COMMAND ----------

# DBTITLE 1,Python Code to Display Top 10 Orders
orders.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Spark SQL Functions
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Timestamp to Date

# COMMAND ----------

# DBTITLE 1,Create Order Details Table
order_details = \
orders. \
select( 
    'order_id', 
    to_date(date_trunc('day', orders['order_timestamp'])).alias('ORDER_DATE'), 
    'customer_id',
    'store_name'
    )

# COMMAND ----------

# DBTITLE 1,Order Details Display
order_details.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Total Order Amount

# COMMAND ----------

# DBTITLE 1,Limit and Display Order Items
order_items.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Calculating Total Order Amount
order_join = \
order_items. \
withColumn(
    'TOTAL_ORDER_AMOUNT',
    round(order_items['unit_price'] * order_items['quantity'], 2)
    ). \
groupBy('order_id'). \
sum('total_order_amount')

# COMMAND ----------

# DBTITLE 1,Inner Join of Order Details
order_join = order_details.join(order_join, order_details['order_id'] == order_join['order_id'], 'inner')

# COMMAND ----------

# DBTITLE 1,Order Details with Total Order Amount
order_details = \
order_join. \
select(
    order_details['ORDER_ID'],
    'ORDER_DATE', 
    'CUSTOMER_ID',
    'STORE_NAME',
    round('sum(total_order_amount)', 2).alias('TOTAL_ORDER_AMOUNT')
    )

# COMMAND ----------

# DBTITLE 1,Display First 11 Order Details.
order_details.limit(11).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monthly Sales Table

# COMMAND ----------

monthly_sales = order_details. \
select(date_format('ORDER_DATE', 'yyyy-MM').alias('MONTH_YEAR'),
       'TOTAL_ORDER_AMOUNT'). \
groupBy('MONTH_YEAR'). \
sum('TOTAL_ORDER_AMOUNT'). \
withColumnRenamed('sum(TOTAL_ORDER_AMOUNT)', 'TOTAL_SALES')

# COMMAND ----------

monthly_sales = \
monthly_sales. \
select(
    'MONTH_YEAR',
    round('TOTAL_SALES', 2).alias('TOTAL_SALES')
    ). \
sort(monthly_sales['MONTH_YEAR'].desc())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Monthly Sales

# COMMAND ----------

order_details.limit(10).display()

# COMMAND ----------

store_monthly_sales = \
order_details. \
select(
    date_format('ORDER_DATE', 'yyyy-MM').alias('MONTH_YEAR'),
    'STORE_NAME',
    'TOTAL_ORDER_AMOUNT'
    ). \
groupBy('MONTH_YEAR', 'STORE_NAME'). \
sum('TOTAL_ORDER_AMOUNT'). \
withColumnRenamed('sum(TOTAL_ORDER_AMOUNT)', 'TOTAL_SALES')

# COMMAND ----------

store_monthly_sales = \
store_monthly_sales. \
select(
    'MONTH_YEAR',
    'STORE_NAME',
    round('TOTAL_SALES', 2).alias('TOTAL_SALES')
    ). \
sort(store_monthly_sales['MONTH_YEAR'].desc())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Parquet Files

# COMMAND ----------

# DBTITLE 1,Parquet File Writing Operations
order_details.write.parquet('dbfs:/FileStore/tables/Gold/order_details', mode = 'overwrite')
monthly_sales.write.parquet('dbfs:/FileStore/tables/Gold/monthly_sales', mode = 'overwrite')
store_monthly_sales.write.parquet('dbfs:/FileStore/tables/Gold/store_monthly_sales', mode = 'overwrite')

# COMMAND ----------

