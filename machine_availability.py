# Databricks notebook source
# MAGIC %md # Load raw machine data
# MAGIC In this example we have a single shift data for 2 lines - L01 and L02. L01 is straight forward starting and ending exactly with the shift without observing any downtime.
# MAGIC 
# MAGIC The line L02 emitted data between 11:15 and 15:45 (4.5 hours). It observed failure starting at 15:15 and stabilized back at 15:45 i.e. half an hour of downtime. This means that L02 operated for 4 hours in the shift. Since the shift length was 8 hours, the L02 availability is hence calculated as 0.5.

# COMMAND ----------

from pyspark.sql import functions as F, Window as W

dfRawData = sqlContext.createDataFrame([
  ("L01", "2022-02-10 08:00:00.000", None),
  ("L02", "2022-02-10 11:15:00.000", None),
  ("L02", "2022-02-10 15:10:00.000", None),
  ("L02", "2022-02-10 15:15:00.000", "f01"),
  ("L02", "2022-02-10 15:20:00.000", "f02"),
  ("L02", "2022-02-10 15:45:00.000", None),
  ("L01", "2022-02-10 16:00:00.000", None)
], ["lineName", "time", "faultCode"]) \
.withColumn("time", F.to_timestamp("time"))

display(dfRawData)

# COMMAND ----------

# MAGIC %md Add previous row timestamp and fault code

# COMMAND ----------

perLineByTimeWindow = W.partitionBy("lineName").orderBy("time")

dfRawData \
    .withColumn("prevTime", F.lag("time").over(perLineByTimeWindow)) \
    .withColumn("prevFaultCode", F.lag("faultCode").over(perLineByTimeWindow)) \
    .createOrReplaceTempView("MachineData")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MachineData

# COMMAND ----------

# MAGIC %md Load shifts metadata

# COMMAND ----------

sqlContext.createDataFrame([
  ("day", 8, 16),
  ("evening", 16, 24),
  ("night", 0, 8)
], ["shiftName", "startHour", "endHour"]) \
.withColumn("totalHours", F.col("endHour") - F.col("startHour")) \
.createOrReplaceTempView("Shifts")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Shifts

# COMMAND ----------

# MAGIC %md Calculate downtime per row if there was fault in previous row

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   d.lineName,
# MAGIC   d.time,
# MAGIC   d.prevTime,
# MAGIC   d.faultCode,
# MAGIC   d.prevFaultCode,
# MAGIC   CASE WHEN prevFaultCode IS NOT NULL THEN to_unix_timestamp(time) - to_unix_timestamp(prevTime) ELSE 0 END AS downTime  
# MAGIC FROM MachineData d
# MAGIC INNER JOIN Shifts s ON hour(d.time) >= s.startHour AND hour(d.time) < s.endHour

# COMMAND ----------

# MAGIC %md Final Result

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   lineName,
# MAGIC   (max(unixTime) - min(unixTime) - sum(downTime)) / 3600 / first(totalHours) AS availability
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT 
# MAGIC     d.lineName,
# MAGIC     to_unix_timestamp(d.time) AS unixTime,
# MAGIC     s.totalHours,
# MAGIC     CASE WHEN prevFaultCode IS NOT NULL THEN to_unix_timestamp(time) - to_unix_timestamp(prevTime) ELSE 0 END AS downTime  
# MAGIC   FROM MachineData d
# MAGIC   INNER JOIN Shifts s ON hour(d.time) >= s.startHour AND hour(d.time) < s.endHour
# MAGIC )
# MAGIC GROUP BY lineName
