# Databricks notebook source
# MAGIC %md ### This notebook reads "body" from first Event Hub message and extracts JSON schema

# COMMAND ----------

import json

connectionString = "<EVENTHUBS_CONNECTIONSTRING>;EntityPath=<EVENTHUB_NAME>"

earliest = {
  "offset": "-1",  
  "seqNo": -1, 
  "enqueuedTime": None, 
  "isInclusive": True
}

ehConf = {
  "eventhubs.connectionString": connectionString,
  "eventhubs.startingPosition": json.dumps(earliest)
}

# COMMAND ----------

rawData = spark.read.format("eventhubs").options(**ehConf).load()

dfBody = rawData.selectExpr("CAST(body AS STRING)")
firstRecordString = dfBody.first()["body"]
firstRecordRDD = sc.parallelize([firstRecordString])

# Create new DataFrame from body's JSON
dfJson = spark.read.json(firstRecordRDD)

# Infer schema from the DataFrame
schema = dfJson.schema.json()

# Store schema in DBFS
schemaFile = "/schema.json"
dbutils.fs.rm(schemaFile)
dbutils.fs.put(schemaFile, schema)

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------
from pyspark.sql.types import StructType

with open("/dbfs/schema.json") as jsonData:
    schema = StructType.fromJson(json.load(jsonData))

print(schema)
