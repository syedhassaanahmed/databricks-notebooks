# Databricks notebook source
# MAGIC %md # Azure Event Hubs -> XML to JSON
# MAGIC This notebook demonstrates reading XML events from Azure Event Hub and writing them to another Event Hub as JSON. In order to run this notebook successfully please install latest version of the following libraries and attach them to the Databricks cluster.
# MAGIC - [azure-eventhubs-spark](https://github.com/Azure/azure-event-hubs-spark)
# MAGIC - [xmltodict](https://github.com/martinblech/xmltodict)

# COMMAND ----------

readConnectionString = "<EVENTHUBS_CONNECTIONSTRING>;EntityPath=<INPUT_EVENTHUB_NAME>"

readEhConf = {
  "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(readConnectionString),
  "eventhubs.consumerGroup": "$Default"
}

# COMMAND ----------

xmlStream = spark.readStream \
  .format("eventhubs") \
  .options(**readEhConf) \
  .load() \
  .selectExpr("CAST(body as STRING) AS xmlBody")

# COMMAND ----------

import xmltodict, json
from pyspark.sql.types import StringType

def xml_to_json_func(s):
  return json.dumps(xmltodict.parse(s))

xml_to_json = udf(xml_to_json_func, StringType())

# COMMAND ----------

processed = xmlStream.withColumn("jsonBody", xml_to_json("xmlBody"))
display(processed)

# COMMAND ----------

writeConnectionString = "<EVENTHUBS_CONNECTIONSTRING>;EntityPath=<OUTPUT_EVENTHUB_NAME>"

writeEhConf = {
  "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(writeConnectionString),
  "eventhubs.consumerGroup": "$Default"
}

# COMMAND ----------

# MAGIC %md **Note:** Use Blob Storage/ADLS Gen2 for [checkpointing](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md#recovering-from-failures-with-checkpointing) in production.

# COMMAND ----------

processed \
  .selectExpr("jsonBody AS body") \
  .writeStream \
  .format("eventhubs") \
  .options(**writeEhConf) \
  .option("checkpointLocation", "///output.txt") \
  .start()

# COMMAND ----------


