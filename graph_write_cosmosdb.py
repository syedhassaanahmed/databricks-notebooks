# Databricks notebook source
# MAGIC %md # Write GraphFrames to Azure Cosmos DB Gremlin API
# MAGIC Requires [graphframes](https://spark-packages.org/package/graphframes/graphframes) and [azure-cosmosdb-spark (uber jar)](http://repo1.maven.org/maven2/com/microsoft/azure/azure-cosmosdb-spark_2.3.0_2.11/1.2.0/) libraries to be uploaded and attached to the cluster

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

from graphframes import *
g = GraphFrame(v, e)
display(g.vertices)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md ## Convert Vertices and Edges to Cosmos DB internal format
# MAGIC Cosmos DB Gremlin API internally keeps a JSON document representation of Edges and Vertices [as explained here](https://vincentlauzon.com/2017/09/05/hacking-accessing-a-graph-in-cosmos-db-with-sql-documentdb-api/).

# COMMAND ----------

def to_cosmosdb_vertices(dfVertices, vertexLabel, partitionKey = ""):
  columns = ["id"]
  
  if partitionKey:
    columns.append(partitionKey)
  
  columns.extend(['array(named_struct("id", uuid(), "_value", {x})) AS {x}'.format(x=x) \
                for x in dfVertices.columns if x not in columns])
 
  return dfVertices.selectExpr(*columns).withColumn("label", lit(vertexLabel))

# COMMAND ----------

cosmosDbVertices = to_cosmosdb_vertices(g.vertices, "person")
display(cosmosDbVertices)

# COMMAND ----------

def to_cosmosdb_edges(g, edgeLabelColumn, partitionKey = ""): 
  dfEdges = g.edges
  
  if partitionKey:
    dfEdges = dfEdges.alias("e") \
      .join(g.vertices.alias("sv"), col("e.src") == col("sv.id")) \
      .join(g.vertices.alias("dv"), col("e.dst") == col("dv.id")) \
      .selectExpr("e.*", "sv." + partitionKey, "dv." + partitionKey + " AS _sinkPartition")

  dfEdges = dfEdges \
    .withColumn("_isEdge", lit(True)) \
    .withColumnRenamed("src", "_vertexId") \
    .withColumnRenamed("dst", "_sink") \
    .withColumnRenamed(edgeLabelColumn, "label")
  
  return dfEdges

# COMMAND ----------

cosmosDbEdges = to_cosmosdb_edges(g.edges, "relationship")
display(cosmosDbEdges)

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


