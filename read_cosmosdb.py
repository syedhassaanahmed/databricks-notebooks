# Databricks notebook source
# MAGIC %md # Read from Azure Cosmos DB

# COMMAND ----------

# MAGIC %md [Create and attach required libraries](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/cosmosdb-connector.html#create-and-attach-required-libraries)
# MAGIC 
# MAGIC OR
# MAGIC 
# MAGIC Using Maven coordinates e.g. `azure-cosmosdb-spark_2.2.0_2.11`
# MAGIC 
# MAGIC ### Store Cosmos DB Secrets (one-time)
# MAGIC ```
# MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
# MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/scopes/create \
# MAGIC   -d '{"scope": "cosmosDb"}'
# MAGIC ```
# MAGIC 
# MAGIC ```
# MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
# MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/secrets/write \
# MAGIC   -d '{"scope": "cosmosDb", "key": "endpoint", "string_value": "<COSMOS DB URI>"}'
# MAGIC ```
# MAGIC 
# MAGIC ```
# MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
# MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/secrets/write \
# MAGIC   -d '{"scope": "cosmosDb", "key": "masterKey", "string_value": "<PRIMARY KEY>"}'
# MAGIC ```

# COMMAND ----------

# MAGIC %md ### Create DataFrames
# MAGIC [Configuration references](https://github.com/Azure/azure-cosmosdb-spark/wiki/Configuration-references)

# COMMAND ----------

cosmosDbEndpoint = dbutils.preview.secret.get(scope = "cosmosDb", key = "endpoint")
cosmosDbMasterKey = dbutils.preview.secret.get(scope = "cosmosDb", key = "masterKey")

cosmosDbConfig = {
  "Endpoint" : cosmosDbEndpoint,
  "Masterkey" : cosmosDbMasterKey,
  "Database" : "db1",
  "query_pagesize" : "2147483647"
}

cosmosDbConfig["Collection"] = "collection1"
df1 = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**cosmosDbConfig).load()

cosmosDbConfig["Collection"] = "collection2"
df2 = spark.read.format("com.microsoft.azure.cosmosdb.spark").options(**cosmosDbConfig).load()

# COMMAND ----------


