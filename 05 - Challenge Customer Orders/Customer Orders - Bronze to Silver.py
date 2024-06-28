# Databricks notebook source
customers = spark.read.csv('dbfs:/FileStore/tables/bronze/customers.csv', header = True)
order_items = spark.read.csv('dbfs:/FileStore/tables/bronze/order_items.csv', header = True)
orders = spark.read.csv('dbfs:/FileStore/tables/bronze/orders.csv', header = True)
products = spark.read.csv('dbfs:/FileStore/tables/bronze/products.csv', header = True)
stores = spark.read.csv('dbfs:/FileStore/tables/bronze/stores.csv', header = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1/ Customers Table

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

# COMMAND ----------

customers_path = 'dbfs:/FileStore/tables/bronze/customers.csv'

customers_schema = StructType([
                    StructField('CUSTOMER_ID', IntegerType(), False),
                    StructField('FULL_NAME', StringType(), False),
                    StructField('EMAIL_ADDRESS', StringType(), False)   
                    ]
                    )

customers = spark.read.csv(path = customers_path, header = True, schema = customers_schema)

# COMMAND ----------

customers.display()

# COMMAND ----------

customers.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2/ Order Items Table

# COMMAND ----------

order_items_path = 'dbfs:/FileStore/tables/bronze/order_items.csv'

order_items_schema = StructType([
                    StructField('ORDER_ID', IntegerType(), False),
                    StructField('LINE_ITEM_ID', IntegerType(), False),
                    StructField('PRODUCT_ID', IntegerType(), False),
                    StructField('UNIT_PRICE', DoubleType(), False),
                    StructField('QUANTITY', IntegerType(), False)  
                    ]
                    )

order_items = spark.read.csv(path = order_items_path, header = True, schema = order_items_schema)

# COMMAND ----------

order_items.display()

# COMMAND ----------

order_items.dtypes

# COMMAND ----------

order_items = order_items.drop('LINE_ITEM_ID')

# COMMAND ----------

order_items.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3/ Products Table

# COMMAND ----------

products_path = 'dbfs:/FileStore/tables/bronze/products.csv'

products_schema = StructType([
                    StructField('PRODUCT_ID', IntegerType(), False),
                    StructField('PRODUCT_NAME', StringType(), False),
                    StructField('UNIT_PRICE', DoubleType(), False),
                    ]
                    )

products = spark.read.csv(path = products_path, header = True, schema = products_schema)

# COMMAND ----------

products.display()

# COMMAND ----------

products.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4/ Orders Table

# COMMAND ----------

orders_path = 'dbfs:/FileStore/tables/bronze/orders.csv'

orders_schema = StructType([
                    StructField('ORDER_ID', IntegerType(), False),
                    StructField('ORDER_DATETIME', StringType(), False),
                    StructField('CUSTOMER_ID', IntegerType(), False),
                    StructField('ORDER_STATUS', StringType(), False),
                    StructField('STORE_ID', IntegerType(), False)  
                    ]
                    )

orders = spark.read.csv(path = orders_path, header = True, schema = orders_schema)

# COMMAND ----------

stores.display()

# COMMAND ----------

stores_path = 'dbfs:/FileStore/tables/bronze/stores.csv'

stores_schema = StructType([
                    StructField('STORE_ID', IntegerType(), False),
                    StructField('STORE_NAME', StringType(), False),
                    StructField('WEB_ADDRESS', IntegerType(), True),
                    StructField('LATITUDE', DoubleType(), True),
                    StructField('LONGITUDE', DoubleType(), True)  
                    ]
                    )

stores = spark.read.csv(path = stores_path, header = True, schema = stores_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Datetime Column

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

# COMMAND ----------

orders = orders.withColumn('ORDER_TIMESTAMP', to_timestamp(orders['order_datetime'], 'dd-MMM-yy hh.mm.ss.SS'))

# COMMAND ----------

orders.display()

# COMMAND ----------

orders.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Join Store Name column

# COMMAND ----------

stores = stores.drop('WEB_ADDRESS', 'LATITUDE', 'LONGITUDE')

# COMMAND ----------

stores.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Filter by order type

# COMMAND ----------

orders = orders.filter(orders['order_status'] == 'COMPLETE')

# COMMAND ----------

orders.count()

# COMMAND ----------

orders = orders.join(stores, orders['store_id'] == stores['store_id'], 'left').drop('order_status', 'order_datetime', 'store_id')b

# COMMAND ----------

orders.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## To parquet Files

# COMMAND ----------

dbutils.fs.rm('dbfs:/FileStore/tables/Silver/orders', recurse = True)
dbutils.fs.rm('dbfs:/FileStore/tables/Silver/order_items', recurse = True)
dbutils.fs.rm('dbfs:/FileStore/tables/Silver/customers', recurse = True)
dbutils.fs.rm('dbfs:/FileStore/tables/Silver/products', recurse = True)

# COMMAND ----------

orders.write.parquet('dbfs:/FileStore/tables/Silver/orders', mode = 'overwrite')
order_items.write.parquet('dbfs:/FileStore/tables/Silver/order_items', mode = 'overwrite')
customers.write.parquet('dbfs:/FileStore/tables/Silver/customers', mode = 'overwrite')
products.write.parquet('dbfs:/FileStore/tables/Silver/products', mode = 'overwrite')

# COMMAND ----------

