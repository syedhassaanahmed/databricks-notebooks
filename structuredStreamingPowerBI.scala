// Databricks notebook source
// MAGIC %md #### Sink to Power BI Streaming Dataset from a Spark Structured Stream using ForeachWriter and [Power BI REST API](https://docs.microsoft.com/en-us/power-bi/service-real-time-streaming#pushing-data-to-datasets)

// COMMAND ----------

import scala.util.parsing.json.JSONObject
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder

class PowerBISink() extends ForeachWriter[Row] {
  var httpClient:HttpClient = _
  var post:HttpPost = _

  def open(partitionId: Long,version: Long): Boolean = {
    httpClient = HttpClientBuilder.create().build()
    post = new HttpPost("https://api.powerbi.com/beta/<WORKSPACE_ID>/datasets/<DATASET_ID>/rows?key=<AUTH_KEY>")
    post.setHeader("Content-type", "application/json")

    true
  }
  
  def process(value: Row): Unit = {        
    val rowMap = value.getValuesMap(value.schema.fieldNames)       
    val jsonObject = JSONObject(rowMap).toString()
    val jsonArray = s"[$jsonObject]"

    post.setEntity(new StringEntity(jsonArray))
    val response = httpClient.execute(post)
  }

  def close(errorOrNull: Throwable): Unit = {        
  }
}

import org.apache.spark.sql.streaming.Trigger

val writer = new PowerBISink()
val df = spark.readStream.format("...").options(...).load()

df.writeStream
  .foreach(writer)
  .trigger(Trigger.ProcessingTime("1 second"))
  .start()
