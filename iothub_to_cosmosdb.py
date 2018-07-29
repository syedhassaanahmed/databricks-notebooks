# Databricks notebook source
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


