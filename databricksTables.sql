-- Databricks notebook source
-- MAGIC %md # [Managed vs Unmanaged Tables](https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables)

-- COMMAND ----------

-- MAGIC %md Copy csv from samples to dbfs root

-- COMMAND ----------

-- MAGIC %fs cp /databricks-datasets/samples/population-vs-price/data_geo.csv /

-- COMMAND ----------

-- MAGIC %md List dbfs root

-- COMMAND ----------

-- MAGIC %fs ls /

-- COMMAND ----------

-- MAGIC %md Create Table with Path specified

-- COMMAND ----------

DROP TABLE IF EXISTS data_geo;

CREATE TABLE data_geo
USING CSV
OPTIONS (path="/data_geo.csv")

-- COMMAND ----------

-- MAGIC %md Alternatively Create Table without Path specified

-- COMMAND ----------

DROP TABLE IF EXISTS data_geo;

CREATE TABLE data_geo
AS SELECT * FROM CSV.`/data_geo.csv`

-- COMMAND ----------

-- MAGIC %md Check row count

-- COMMAND ----------

SELECT count(*) FROM data_geo

-- COMMAND ----------

-- MAGIC %md Now remove the source file

-- COMMAND ----------

-- MAGIC %fs rm data_geo.csv

-- COMMAND ----------

REFRESH TABLE data_geo

-- COMMAND ----------

-- MAGIC %md Check count again

-- COMMAND ----------


