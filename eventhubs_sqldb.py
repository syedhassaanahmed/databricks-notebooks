# Databricks notebook source
# MAGIC %md #### Ingest stream from Event Hubs and sink to Azure SQL DB using ForeachWriter
# MAGIC Before running this notebook, make sure [azure-eventhubs-spark](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md#linking) library is uploaded and attached to the cluster.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

connectionString = "<EVENTHUBS_CONNECTIONSTRING>;EntityPath=<EVENTHUB_NAME>"

ehConf = {
  "eventhubs.connectionString": connectionString,
  "eventhubs.consumerGroup": "<EHConsumerGroup>"
}

# COMMAND ----------

inputStream = spark.readStream.format("eventhubs").options(**ehConf).load()
  
# Cast the data as string (it comes in as binary by default)
bodyNoSchema = inputStream.selectExpr("CAST(body as STRING)")

# COMMAND ----------

# Define the schema to apply to the data...
schema = StructType([
  StructField("field1", StringType()),
  StructField("field2", StringType(), True)
])

# Apply the schema...
bodyWithSchema = bodyNoSchema.select(from_json(col("body"), schema).alias("data"))

# Filter down to just the data that we're looking for...
# Watermarking to account for late arriving events
windowedBody = bodyWithSchema \
  .select("data.*").withColumn("dateTime", col("field2").cast(TimestampType())) \
  .withWatermark("dateTime", "5 minutes")
  
display(windowedBody)

windowedBody.createOrReplaceTempView("AllData")

# COMMAND ----------

# MAGIC %scala
# MAGIC import java.sql.DriverManager
# MAGIC import org.apache.spark.sql.ForeachWriter
# MAGIC 
# MAGIC val dfPersistence = spark.sql("SELECT * FROM AllData")
# MAGIC display(dfPersistence)
# MAGIC 
# MAGIC // Writing to SQL
# MAGIC val persistenceQuery = dfPersistence.writeStream.foreach(new ForeachWriter[Row] {
# MAGIC   var connection:java.sql.Connection = _
# MAGIC   var statement:java.sql.Statement = _
# MAGIC   
# MAGIC   // TODO: Replace these values as necessary...
# MAGIC   val tableName = "dbo.<TableName>"
# MAGIC   val serverName = "<SQLServerName>.database.windows.net"
# MAGIC   val jdbcPort = 1433
# MAGIC   val database ="<SQLDatabaseName>"
# MAGIC   val writeuser = "<SQLDatabaseUser>"
# MAGIC   val writepwd = "<SQLDatabasePassword>"
# MAGIC 
# MAGIC   val driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
# MAGIC   val jdbc_url = s"jdbc:sqlserver://${serverName}:${jdbcPort};database=${database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
# MAGIC   
# MAGIC    def open(partitionId: Long, version: Long):Boolean = {
# MAGIC     Class.forName(driver)
# MAGIC     connection = DriverManager.getConnection(jdbc_url, writeuser, writepwd)
# MAGIC     statement = connection.createStatement
# MAGIC     true
# MAGIC   }
# MAGIC   
# MAGIC   def process(value: Row): Unit = {
# MAGIC     val field1 = value(0)
# MAGIC     val field2 = value(1)
# MAGIC     
# MAGIC     val valueStr = s"'${field1}', '${field2}'"
# MAGIC     val statementStr = s"INSERT INTO ${tableName} (column1, column2) VALUES (${valueStr})"
# MAGIC 
# MAGIC     statement.execute(statementStr)
# MAGIC   }
# MAGIC 
# MAGIC   def close(errorOrNull: Throwable): Unit = {
# MAGIC     connection.close
# MAGIC   }
# MAGIC })
# MAGIC 
# MAGIC val streamingQuery = persistenceQuery.start()

# COMMAND ----------

# MAGIC %scala
# MAGIC streamingQuery.stop()

# COMMAND ----------


