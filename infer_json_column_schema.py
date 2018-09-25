# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# COMMAND ----------

exampleJSON = """
{
  "a": "{\\"b\\":{\\"x\\":1,\\"y\\":{\\"z\\":2}}}"
}
"""

dfExampleJSON = spark.read.json(sc.parallelize([exampleJSON]))
display(dfExampleJSON)

# COMMAND ----------

def get_json_schema(df, jsonColumnName):
  firstRecordString = df.where(col(jsonColumnName).isNotNull()).first()[jsonColumnName]
  firstRecordRDD = sc.parallelize([firstRecordString])
  dfJSON = spark.read.json(firstRecordRDD)
  schema = dfJSON.schema.json()
  dictionary = json.loads(schema)
  
  return StructType.fromJson(dictionary)

# COMMAND ----------

schema = get_json_schema(dfExampleJSON, "a")
dfExampleStruct = dfExampleJSON.withColumn("a", from_json("a", schema))

display(dfExampleStruct)
