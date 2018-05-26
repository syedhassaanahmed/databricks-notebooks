// Databricks notebook source
// MAGIC %md ## How to run latest Hive UDFs in Spark
// MAGIC 
// MAGIC ###Problem:
// MAGIC `GenericUDFMask` was [added](https://issues.apache.org/jira/browse/HIVE-13568) in Hive 2.1.0. However Spark comes bundled with Hive 1.2.1, so this UDF and others in this version are not accessible in Spark. This issue is currently being [tracked here](https://issues.apache.org/jira/browse/SPARK-23901).
// MAGIC  
// MAGIC ###Solution:
// MAGIC Download [this jar](https://mvnrepository.com/artifact/org.apache.hive/hive-exec), manually upload it to Create a Library (Maven coordinate won't work) and attach it to your cluster.

// COMMAND ----------

import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sql("""CREATE TEMPORARY FUNCTION mask_hash AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskHash'""")

// COMMAND ----------

// MAGIC %sql SELECT mask_hash("example@email.com") AS masked

// COMMAND ----------


