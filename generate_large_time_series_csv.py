# Databricks notebook source
from pyspark.sql.functions import *
import time

days_back = 1
values_per_second = 337

sqlContext.range(0, days_back * 24 * 60 * 60 * values_per_second) \
  .withColumn("seconds", col("id") / values_per_second) \
  .withColumn("nowTimestamp", lit(time.time())) \
  .createOrReplaceTempView("all_rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW TimeSeries AS
# MAGIC SELECT
# MAGIC   CAST((nowTimestamp - seconds) AS Timestamp) AS Timestamp
# MAGIC   , concat_ws('-', 1 + CAST(rand() * 10 AS Int), 1 + CAST(rand() * 100 AS Int), 1 + CAST(rand() * 350 AS Int)) AS Sensor
# MAGIC   , round(rand() * 100, 3) AS Value
# MAGIC FROM all_rows

# COMMAND ----------

dfTimeSeries = spark.table("TimeSeries") \
  .withColumn("year", year(col("Timestamp"))) \
  .withColumn("month", month(col("Timestamp"))) \
  .withColumn("day", dayofmonth(col("Timestamp")))

display(dfTimeSeries)

# COMMAND ----------

spark.conf.set("fs.azure.account.key.<StorageAccountName>.blob.core.windows.net", \
  "<StorageAccountKey>")

dfTimeSeries.write \
  .mode("overwrite") \
  .partitionBy("year", "month", "day") \
  .csv("wasbs://<StorageContainer>@<StorageAccountName>.blob.core.windows.net/timeseries")

# COMMAND ----------


