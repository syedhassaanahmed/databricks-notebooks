# Databricks notebook source
# MAGIC %md #### For best performance use 5 instances of `Standard_DS15_v2` (Memory Optimized) as Worker Node

# COMMAND ----------

import pyspark.sql.functions as F
import time

days_back = 120
industrial_plants = 50

start_time = time.time()

# COMMAND ----------

def get_sensors(industrial_plants, sensors_per_plant):
  return spark.range(0, industrial_plants * sensors_per_plant) \
    .selectExpr("id as RowNum", "uuid() AS Sensor") \
    .withColumn("plant", F.col("RowNum") % industrial_plants)

# COMMAND ----------

def generate_timeseries(df_sensors, start_time, days_back, sensor_frequency):
  total_sensors = df_sensors.count()
  values_per_second = total_sensors * sensor_frequency
    
  return spark.range(0, days_back * 24 * 60 * 60 * values_per_second, 1, 8000) \
    .withColumn("RowNum", F.col("id") % total_sensors) \
    .join(df_sensors, "RowNum") \
    .withColumn("Timestamp", (start_time - (F.col("id") / values_per_second)).cast("Timestamp")) \
    .withColumn("Value", F.round(F.rand() * 100, 16)) \
    .withColumn("year", F.year("Timestamp")) \
    .withColumn("month", F.month("Timestamp")) \
    .withColumn("day", F.dayofmonth("Timestamp")) \
    .drop("id", "RowNum")

# COMMAND ----------

sensors_per_plant = 5000
df_sensors = get_sensors(industrial_plants, sensors_per_plant)
display(df_sensors)

# COMMAND ----------

sensor_frequency = 1/10
dfTimeseries = generate_timeseries(df_sensors, start_time, days_back, sensor_frequency)
display(dfTimeseries)

# COMMAND ----------

spark.conf.set("fs.azure.account.key.<StorageAccountName>.blob.core.windows.net", \
  "<StorageAccountKey>")

blob_root = "wasbs://<StorageContainer>@<StorageAccountName>.blob.core.windows.net"

# COMMAND ----------

dfTimeseries.write \
  .mode("overwrite") \
  .partitionBy("plant", "year", "month", "day") \
  .csv(blob_root)
