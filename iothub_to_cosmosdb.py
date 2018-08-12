# Databricks notebook source
# MAGIC %md # Azure IoT Hub -> Azure Cosmos DB
# MAGIC This notebook demonstrates reading IoT events from an Azure IoT Hub and writes these raw events into an Azure Cosmos DB Collection.
# MAGIC In order to run this notebook successfully, the following connectors are required.
# MAGIC - [azure-eventhubs-spark](https://github.com/Azure/azure-event-hubs-spark)
# MAGIC - [azure-cosmosdb-spark (uber jar)](https://github.com/Azure/azure-cosmosdb-spark#using-databricks-notebooks)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

connectionString = "Endpoint=sb://<EVENTHUB_COMPATIBLE_ENDPOINT>.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=<SHARED_ACCESS_KEY>;EntityPath=<IOTHUB_NAME>"

ehConf = {
  "eventhubs.connectionString": connectionString,
  "eventhubs.consumerGroup": "$Default"
}

# COMMAND ----------

inputStream = spark.readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()
  
bodyNoSchema = inputStream.selectExpr("CAST(body as STRING)")

# COMMAND ----------

schema = StructType([
  StructField("messageId", IntegerType()),
  StructField("deviceId", StringType()),
  StructField("temperature", DoubleType()),
  StructField("humidity", DoubleType())
])

bodyWithSchema = bodyNoSchema.select(col("body"), from_json(col("body"), schema).alias("data"))
ehStream = bodyWithSchema.select("data.*")


# COMMAND ----------

display(ehStream)

# COMMAND ----------

cosmosDbConfig = {
  "Endpoint" : "https://<COSMOSDB_ENDPOINT>.documents.azure.com:443/",
  "Masterkey" : "<COSMOSDB_PRIMARYKEY>",
  "Database" : "<DATABASE>",
  "Collection" : "<COLLECTION>",
  "Upsert" : "true"
}

cosmosDbStreamWriter = ehStream \
  .writeStream \
  .outputMode("append") \
  .format("com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSinkProvider") \
  .options(**cosmosDbConfig) \
  .option("checkpointLocation", "/tmp/streamingCheckpoint") \
  .start()

# COMMAND ----------


