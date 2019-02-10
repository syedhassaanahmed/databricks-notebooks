-- Databricks notebook source
CREATE TABLE IF NOT EXISTS CosmosDBStream(column1 int, column2 string, column3 timestamp)
USING com.microsoft.azure.cosmosdb.spark
OPTIONS
(
  endpoint "https://<COSMOSDB_ENDPOINT>.azure.com:443/", 
  database "<DATABASE>", 
  collection "<COLLECTION>", 
  masterkey "<COSMOSDB_KEY>"
)

-- COMMAND ----------

-- MAGIC %md #### Rerunning the following query should give higher count every time documents are inserted in above collection

-- COMMAND ----------

SELECT count(1) FROM CosmosDBStream

-- COMMAND ----------


