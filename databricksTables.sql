-- Databricks notebook source
-- MAGIC %md # [Managed vs Unmanaged Tables](https://docs.databricks.com/user-guide/tables.html#managed-and-unmanaged-tables)

-- COMMAND ----------

-- MAGIC %fs cp /databricks-datasets/samples/population-vs-price/data_geo.csv /

-- COMMAND ----------

-- MAGIC %fs ls /

-- COMMAND ----------

DROP TABLE IF EXISTS data_geo;

CREATE TABLE data_geo
USING CSV
OPTIONS (path="/data_geo.csv")

-- COMMAND ----------

DROP TABLE IF EXISTS data_geo;

CREATE TABLE data_geo
AS SELECT * FROM CSV.`/data_geo.csv`

-- COMMAND ----------

SELECT count(1) FROM data_geo

-- COMMAND ----------

-- MAGIC %fs rm data_geo.csv

-- COMMAND ----------

REFRESH TABLE data_geo

-- COMMAND ----------

-- MAGIC %md Check count again

-- COMMAND ----------


