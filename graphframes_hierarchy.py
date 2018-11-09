# Databricks notebook source
# MAGIC %md ## Hierarchical data traversal in GraphFrames using [Belief Propagation](https://graphframes.github.io/api/python/_modules/graphframes/examples/belief_propagation.html#BeliefPropagation)
# MAGIC Requires **Spark 2.4**. [Graphframes](https://spark-packages.org/package/graphframes/graphframes) library must be uploaded and attached to the cluster.

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

v = sqlContext.createDataFrame([
  ("1", "CEO"),
  ("2", "CVP"),
  ("3", "GM"),
  ("4", "Mgr"),
  ("5", "SE"),
  ("6", "PM")
], ["id", "name"])

display(v)

# COMMAND ----------

e = sqlContext.createDataFrame([
  ("1", "2"),
  ("2", "3"),
  ("3", "4"),
  ("4", "5"),
  ("4", "6")
], ["src", "dst"])

display(e)

# COMMAND ----------

from graphframes import GraphFrame
from graphframes.lib import AggregateMessages as AM

# Create a graph with vertices containing an empty parents array column
g = GraphFrame(v.withColumn("parents", array()), e)

# Initial message to be passed to neighbor vertices. We want to traverse from the leaf, hence AM.src
msgToDst = AM.src["name"]

for i in range(6):
  # AM.msg contains the next message i.e. next parent in our case
  agg = g.aggregateMessages(
      collect_list(AM.msg).alias("tmpParent"),
      sendToDst = msgToDst)

  # Append this message to the parents array column of vertices and also keep it as a standalone column for next iteration
  currentV = g.vertices
  newV = currentV.join(agg, "id", how = "left") \
    .drop(agg["id"]) \
    .withColumn("parents", concat(agg["tmpParent"], currentV["parents"])) \
    .withColumn("lastParent", col("tmpParent")[0]) \
    .drop("tmpParent")
  
  # Caching the transitionary vertices dataframe is important here, otherwise the Spark job will take very long time to complete
  cachedNewV = AM.getCachedDataFrame(newV)
  g = GraphFrame(cachedNewV, g.edges)
  
  # Pass the standalone column i.e recent parent to the next iteration
  msgToDst = AM.src["lastParent"]

g = GraphFrame(g.vertices.drop("lastParent"), g.edges)
display(g.vertices)

# COMMAND ----------

dfParents = g.vertices.selectExpr("id", "name",
            "map_from_arrays(transform(sequence(1, size(parents)), x -> concat('L', x)), parents) AS parents")

display(dfParents)
