# Databricks notebook source
import pyspark.sql.functions as F
import time

# COMMAND ----------

days_back = 14
values_per_second = 337
nowTimestamp = time.time()

# COMMAND ----------

dfTimeSeries = sqlContext.range(0, days_back * 24 * 60 * 60 * values_per_second) \
  .withColumn("Timestamp", (nowTimestamp - (F.col("id") / values_per_second)).cast("Timestamp")) \
  .drop("id") \
  .withColumn("Sensor", F.concat_ws('-', 
                               1 + (F.rand() * 10).cast("Int"), 
                               1 + (F.rand() * 100).cast("Int"), 
                               1 + (F.rand() * 350).cast("Int"))) \
  .withColumn("Value", F.round(F.rand() * 100, 3)) \
  .withColumn("year", F.year("Timestamp")) \
  .withColumn("month", F.month("Timestamp")) \
  .withColumn("day", F.dayofmonth("Timestamp"))

display(dfTimeSeries)

# COMMAND ----------

spark.conf.set("fs.azure.account.key.<StorageAccountName>.blob.core.windows.net", \
  "<StorageAccountKey>")

dfTimeSeries.write \
  .mode("overwrite") \
  .partitionBy("year", "month", "day") \
  .csv("wasbs://<StorageContainer>@<StorageAccountName>.blob.core.windows.net/timeseries")

# COMMAND ----------


