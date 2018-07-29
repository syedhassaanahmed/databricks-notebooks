# Databricks notebook source
# MAGIC %md # Write Streaming Data to Cosmos DB
# MAGIC Assuming `df` is a streaming DataFrame

# COMMAND ----------

# MAGIC %md ### a) StreamWriter
# MAGIC Following cell will fail until [this issue](https://github.com/Azure/azure-cosmosdb-spark/issues/147) is fixed

# COMMAND ----------

cosmosDbConfig = {
  "Endpoint" : "https://<COSMOSDB_ENDPOINT>.documents.azure.com:443/",
  "Masterkey" : "<COSMOSDB_PRIMARYKEY>",
  "Database" : "<DATABASE>",
  "Collection" : "<COLLECTION>",
  "Upsert" : "true"
}

df.writeStream.format("com.microsoft.azure.cosmosdb.spark").options(**cosmosDbConfig).start().awaitTermination()

# COMMAND ----------

# MAGIC %md ### b) ForeachWriter
# MAGIC Switch to Scala due to lack of PySpark support

# COMMAND ----------

df.createOrReplaceTempView("stream")

# COMMAND ----------

# MAGIC %scala
# MAGIC 
# MAGIC import scala.util.parsing.json.JSONObject
# MAGIC import org.apache.spark.sql.ForeachWriter
# MAGIC import com.microsoft.azure.documentdb.{ConnectionPolicy, ConsistencyLevel, Document, DocumentClient}
# MAGIC 
# MAGIC class CosmosDBSink() extends ForeachWriter[Row] {
# MAGIC      var documentClient:DocumentClient = _
# MAGIC   
# MAGIC      val ENDPOINT = "https://<COSMOSDB_ENDPOINT>.documents.azure.com:443/"
# MAGIC      val MASTER_KEY = "<COSMOSDB_KEY>"
# MAGIC      val DATABASE_ID = "<DATABASE>"
# MAGIC      val COLLECTION_ID = "<COLLECTION>"
# MAGIC       
# MAGIC       def open(partitionId: Long,version: Long): Boolean = {
# MAGIC         documentClient = new DocumentClient(ENDPOINT,
# MAGIC                 MASTER_KEY, ConnectionPolicy.GetDefault(),
# MAGIC                 ConsistencyLevel.Session)
# MAGIC 
# MAGIC         true
# MAGIC       }
# MAGIC   
# MAGIC       def process(value: Row): Unit = {        
# MAGIC         val rowMap = value.getValuesMap(value.schema.fieldNames)        
# MAGIC         val document = new Document(JSONObject(rowMap).toString())
# MAGIC         
# MAGIC         documentClient.createDocument("dbs/" + DATABASE_ID + "/colls/" + COLLECTION_ID, document, null, false)
# MAGIC       }
# MAGIC   
# MAGIC       def close(errorOrNull: Throwable): Unit = {        
# MAGIC       }
# MAGIC }

# COMMAND ----------

val df = spark.read.table("stream")
val writer = new CosmosDBSink()
df.writeStream
  .foreach(writer)
  .start()

# COMMAND ----------


