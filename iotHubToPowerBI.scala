// Databricks notebook source
// MAGIC %md ### Sink to Power BI Streaming Dataset from an IoT Hub Stream using [Power BI REST API](https://docs.microsoft.com/en-us/power-bi/service-real-time-streaming#pushing-data-to-datasets)
// MAGIC **Note:** [azure-event-hubs-spark](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/structured-streaming-eventhubs-integration.md#iot-hub) library must be attached to the cluster.
// MAGIC The simulated telemetry was sent using [this C# sample](https://docs.microsoft.com/en-us/azure/iot-hub/quickstart-send-telemetry-dotnet#send-simulated-telemetry)

// COMMAND ----------

import org.apache.spark.eventhubs._

val connectionString = ConnectionStringBuilder("<EVENTHUB_COMPATIBLE_ENDPOINT>")
  .setEventHubName("<EVENTHUB_COMPATIBLE_NAME>")
  .build

val ehConf = EventHubsConf(connectionString)

// COMMAND ----------

val dfRaw = spark
  .readStream
  .format("eventhubs")
  .options(ehConf.toMap)
  .load()

display(dfRaw)

// COMMAND ----------

import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._

val schema = StructType(List(
    StructField("temperature", DoubleType),
    StructField("humidity", DoubleType))
)

val dfStream = dfRaw.select($"enqueuedTime", from_json($"body".cast("string"), schema).alias("data"))
  .select($"enqueuedTime", $"data.*") 

display(dfStream)

// COMMAND ----------

val dfJsonStream = dfStream.select(
  to_json(
    array(
      struct($"enqueuedTime", $"temperature", $"humidity")
    )
  ).alias("jsonArray")
)

display(dfJsonStream)

// COMMAND ----------

import org.apache.spark.sql.ForeachWriter
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.entity.StringEntity

class PowerBISink(var powerBIEndpoint: String, var jsonArrayColumn: String) extends ForeachWriter[Row] {
  var httpClient:HttpClient = _
  var post:HttpPost = _  

  def open(partitionId: Long,version: Long): Boolean = {
    httpClient = HttpClientBuilder.create().build()
    post = new HttpPost(powerBIEndpoint)
    post.setHeader("Content-type", "application/json")

    true
  }
  
  def process(row: Row): Unit = {        
    val jsonArray = row.getAs[String](jsonArrayColumn)

    post.setEntity(new StringEntity(jsonArray))
    val response = httpClient.execute(post)
  }

  def close(errorOrNull: Throwable): Unit = {        
  }
}

// COMMAND ----------

// MAGIC %md #### Aggregate requests due to [Power BI REST API limitations](https://docs.microsoft.com/en-us/power-bi/developer/api-rest-api-limitations)

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger

val powerBIEndpoint = "https://api.powerbi.com/beta/<WORKSPACE_ID>/datasets/<DATASET_ID>/rows?key=<AUTH_KEY>"

val powerBIWriter = new PowerBISink(powerBIEndpoint, "jsonArray")

dfJsonStream.writeStream
  .foreach(powerBIWriter)
  .trigger(Trigger.ProcessingTime("1 second"))
  .start()
