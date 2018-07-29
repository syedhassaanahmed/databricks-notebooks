# Databricks notebook source
# MAGIC %md # Write GraphFrames to Azure Cosmos DB Gremlin API
# MAGIC Requires [graphframes](https://spark-packages.org/package/graphframes/graphframes) and [azure-cosmosdb-spark (uber jar)](http://repo1.maven.org/maven2/com/microsoft/azure/azure-cosmosdb-spark_2.3.0_2.11/1.2.0/) libraries to be uploaded and attached to the cluster

# COMMAND ----------

# MAGIC %md ## Vertex DataFrame

# COMMAND ----------

v = sqlContext.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)
], ["id", "name", "age"])

# COMMAND ----------

# MAGIC %md ## Edge DataFrame

# COMMAND ----------

e = sqlContext.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
], ["src", "dst", "relationship"])

# COMMAND ----------

# MAGIC %md ## Create a GraphFrame

# COMMAND ----------

from graphframes import *
g = GraphFrame(v, e)
display(g.vertices)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md ## Convert vertices and Edges to Cosmos DB internal format
# MAGIC Cosmos DB Gremlin API internally keeps a JSON document representation of Edges and Vertices [as explained here](https://vincentlauzon.com/2017/09/05/hacking-accessing-a-graph-in-cosmos-db-with-sql-documentdb-api/).

# COMMAND ----------

# MAGIC %md ### Convert Vertices

# COMMAND ----------

import uuid

def to_cosmosdb_vertices(dfVertices, vertexLabel):
  dfCosmosDbVertices = dfVertices
  properties = dfCosmosDbVertices.columns
  properties.remove("id")  
  
  for property in properties:
    uuidUdf = udf(lambda : str(uuid.uuid4()), StringType())
    
    dfCosmosDbVertices = dfCosmosDbVertices.withColumn(property, array(struct(
      uuidUdf().alias("id"), 
      col(property).alias("_value")
    )))
  
  return dfCosmosDbVertices.withColumn("label", lit(vertexLabel))

# COMMAND ----------

cosmosDbVertices = to_cosmosdb_vertices(g.vertices, "person")
display(cosmosDbVertices)

# COMMAND ----------

# MAGIC %md ### Convert Edges

# COMMAND ----------

def to_cosmosdb_edges(dfEdges, edgeLabelColumn):
  return dfEdges.withColumn("_isEdge", lit(True)) \
    .withColumnRenamed("src", "_vertexId") \
    .withColumnRenamed("dst", "_sink") \
    .withColumnRenamed(edgeLabelColumn, "label")

# COMMAND ----------

cosmosDbEdges = to_cosmosdb_edges(g.edges, "relationship")
display(cosmosDbEdges)

# COMMAND ----------

# MAGIC %md ## Insert Vertices and Edges to Cosmos DB

# COMMAND ----------

cosmosDbConfig = {
  "Endpoint" : "https://<COSMOSDB_ENDPOINT>.documents.azure.com:443/",
  "Masterkey" : "<COSMOSDB_PRIMARYKEY>",
  "Database" : "<DATABASE>",
  "Collection" : "<COLLECTION>",
  "Upsert" : "true"
}

cosmosDbFormat = "com.microsoft.azure.cosmosdb.spark"

cosmosDbVertices.write.format(cosmosDbFormat).mode("append").options(**cosmosDbConfig).save()
cosmosDbEdges.write.format(cosmosDbFormat).mode("append").options(**cosmosDbConfig).save()

# COMMAND ----------


