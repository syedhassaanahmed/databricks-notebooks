# Databricks notebook source
# MAGIC %md # Azure Event Hubs -> MongoDB
# MAGIC This notebook demonstrates reading events from Azure Event Hubs and writing them to a MongoDB Collection using [foreachBatch()](https://docs.databricks.com/spark/latest/structured-streaming/foreach.html).
# MAGIC 
# MAGIC In order to run this notebook successfully, the following connectors must be installed and attached to the Databricks cluster.
# MAGIC - [azure-eventhubs-spark](https://github.com/Azure/azure-event-hubs-spark)
# MAGIC - [mongo-spark-connector_2.11
# MAGIC ](https://github.com/mongodb/mongo-spark)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

connectionString = "<EVENTHUBS_CONNECTIONSTRING>;EntityPath=<EVENTHUB_NAME>"

ehConf = {
  "eventhubs.connectionString": connectionString,
  "eventhubs.consumerGroup": "$Default"
}

# COMMAND ----------

inputStream = spark.readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()
  
rawBody = inputStream.selectExpr("CAST(body as STRING)")

# COMMAND ----------

schema = StructType([
  StructField("messageId", IntegerType()),
  StructField("deviceId", StringType()),
  StructField("temperature", DoubleType()),
  StructField("humidity", DoubleType())
])

bodyWithSchema = rawBody.select(col("body"), from_json(col("body"), schema).alias("data"))
ehStream = bodyWithSchema.select("data.*")


# COMMAND ----------

display(ehStream)

# COMMAND ----------

mongoConfig = {
  "uri" : "<MONGO_CONNECTION_STRING> e.g. mongodb://...",
  "database" : "<DATABASE>",
  "collection" : "<COLLECTION>"
}

# COMMAND ----------

def foreach_batch_mongo(df, epoch_id):
  df.write \
    .format("mongo") \
    .mode("append") \
    .options(**mongoConfig) \
    .save()

# COMMAND ----------

# MAGIC %md **Note:** If using Cosmos DB, please make sure to provision appropriate number of RUs in order to avoid Bulk write operation errors.

# COMMAND ----------

ehStream \
  .writeStream \
  .foreachBatch(foreach_batch_mongo) \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/streamingCheckpoint") \
  .start()
  