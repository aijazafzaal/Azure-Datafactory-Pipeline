-- Databricks notebook source
-- MAGIC %python
-- MAGIC a=100

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mount(
-- MAGIC   source= "wasbs://raw@blobstorage95.blob.core.windows.net",
-- MAGIC   mount_point="/mnt/raw",
-- MAGIC   extra_configs={"fs.azure.account.key.blobstorage95.blob.core.windows.net":"MxEkQcMq7qwlN8qSor7ZSmz4hLz4lzxbl536OzbcqlA38xHNC4sEAI+0wnHYkQ6B2CcNTSyj+AStLjfCDw=="})
                                                                                     -- I changed the pin :)
-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC import urllib
-- MAGIC 
-- MAGIC df = spark.read.format("csv")\
-- MAGIC .load("/mnt/raw/Sample - Superstore.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file_location = "/mnt/raw/Sample - Superstore.csv"
-- MAGIC file_type = "csv"
-- MAGIC infer_schema = "true"
-- MAGIC first_row_is_header = "true"
-- MAGIC delimiter = ","
-- MAGIC 
-- MAGIC # The applied options are for CSV files. For other file types these will be ignored
-- MAGIC df = spark.read.format(file_type)\
-- MAGIC     .option("inferschema", infer_schema)\
-- MAGIC     .option("header", first_row_is_header)\
-- MAGIC     .option("sep", delimiter)\
-- MAGIC     .load(file_location)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.columns
-- MAGIC #df.write.mode("overwrite").saveAsTable("superstore")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC renamed_cols = {
-- MAGIC     "Row ID": "Row_ID",
-- MAGIC     "Order ID": "Order_ID",
-- MAGIC     "Order Date": "Order_Date",
-- MAGIC     "Ship Date": "Ship_Date",
-- MAGIC     "Ship Mode": "Ship_Mode",
-- MAGIC     "Customer ID": "Customer_ID",
-- MAGIC     "Customer Name": "Customer_Name",
-- MAGIC     "Postal Code": "Postal_Code",
-- MAGIC     "Product ID": "Product_ID",
-- MAGIC     "Product Name": "Product_Name"
-- MAGIC }
-- MAGIC 
-- MAGIC df = df.select([col(c).alias(renamed_cols.get(c, c)) for c in df.columns])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.columns

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").saveAsTable("superstore")

-- COMMAND ----------

select * from superstore

-- COMMAND ----------

select sum(profit), city from superstore
group by 2

-- COMMAND ----------

select sum(sales), region from superstore
group by 2

-- COMMAND ----------


