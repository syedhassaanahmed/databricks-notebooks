# Databricks notebook source
# MAGIC %md # Stream from Kafka-Enabled Event Hub
# MAGIC Spark Structured Streaming ingest from a Kafka-enabled Azure Event Hub

# COMMAND ----------

jaas = "dbfs:/kafka.jaas"

dbutils.fs.rm(jaas)

dbutils.fs.put(jaas,
"""KafkaClient {
org.apache.kafka.common.security.plain.PlainLoginModule required
username="$ConnectionString"
password="Endpoint=sb://<EVENTHUBS_NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<PRIMARY_KEY>";
};"""
)

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

display(spark.read.text("dbfs:/kafka.jaas"))

# COMMAND ----------

# MAGIC %md ## Create a Java Truststore (.jks) for validating certificates
# MAGIC Here is a sample https://gist.github.com/syedhassaanahmed/46133d4c31e22565baf993901536a423

# COMMAND ----------

# MAGIC %md ## Add the following to cluster config and restart cluster
# MAGIC ```
# MAGIC spark.driver.extraJavaOptions -Djava.security.auth.login.config=/dbfs/kafka.jaas
# MAGIC spark.executor.extraJavaOptions -Djava.security.auth.login.config=/dbfs/kafka.jaas
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## Attach Kafka Clients 1.0 library to the cluster
# MAGIC **Maven Coordinates:** `org.apache.kafka:kafka-clients:1.0.0`

# COMMAND ----------

df = spark.readStream.format("kafka") \
  .option("kafka.bootstrap.servers", "<EVENTHUBS_NAMESPACE>.servicebus.windows.net:9093") \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.ssl.truststore.location","/dbfs/kafka.client.truststore.jks") \
  .option("kafka.ssl.truststore.password","change-me-to-something-safe") \
  .option("subscribe", "kafka-events") \
  .option("startingOffsets", "latest") \
  .load()

# COMMAND ----------

display(df)
