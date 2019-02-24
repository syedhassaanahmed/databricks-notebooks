# Databricks notebook source
# MAGIC %md #### For best performance use 5 instances of `Standard_DS15_v2` (Memory Optimized) as Worker Node

# COMMAND ----------

import pyspark.sql.functions as F
import time

# COMMAND ----------

days_back = 120
industrial_plants = 50
start_time = time.time()

# COMMAND ----------

def generate_timeseries(start_time, days_back, industrial_plants, sensor_type, sensors_per_plant, values_per_second_per_sensor):
  values_per_second = industrial_plants * sensors_per_plant * values_per_second_per_sensor  
  
  return spark.range(0, days_back * 24 * 60 * 60 * values_per_second, 1, 80000) \
    .withColumn("Timestamp", (start_time - (F.col("id") / values_per_second)).cast("Timestamp")) \
    .withColumn("Tag", F.concat_ws('-', 
                                 1 + (F.col("id") / sensors_per_plant % industrial_plants).cast("Int"), 
                                 F.lit(sensor_type), 
                                 1 + (F.col("id") % sensors_per_plant))) \
    .withColumn("Value", F.round(F.rand() * 100, 16)) \
    .withColumn("year", F.year("Timestamp")) \
    .withColumn("month", F.month("Timestamp")) \
    .withColumn("day", F.dayofmonth("Timestamp")) \
    .drop("id")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.<StorageAccountName>.blob.core.windows.net", \
  "<StorageAccountKey>")

blob_root = "wasbs://<StorageContainer>@<StorageAccountName>.blob.core.windows.net"

# COMMAND ----------

dfTimeseriesA = generate_timeseries(start_time, days_back, industrial_plants, "A", 5000, 1/3)
#display(dfTimeseriesA)

# COMMAND ----------

dfTimeseriesA.write \
  .mode("overwrite") \
  .partitionBy("year", "month", "day") \
  .csv(blob_root + "/A")

# COMMAND ----------

dfTimeseriesB = generate_timeseries(start_time, days_back, industrial_plants, "B", 10000, 1/10)
#display(dfTimeseriesB)

# COMMAND ----------

dfTimeseriesB.write \
  .mode("overwrite") \
  .partitionBy("year", "month", "day") \
  .csv(blob_root + "/B")
