# Databricks notebook source
# MAGIC %md # Stream from Kafka-Enabled Event Hub
# MAGIC Spark Structured Streaming ingest from a Kafka-enabled Azure Event Hubs
# MAGIC 
# MAGIC **Note:** Spark 2.4 is required

# COMMAND ----------

eh_sasl = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="Endpoint=sb://<EVENTHUBS_NAMESPACE>.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<PRIMARY_KEY>";'

df = spark.readStream \
  .format("kafka") \
  .option("subscribe", "test-topic") \
  .option("kafka.bootstrap.servers", "<EVENTHUBS_NAMESPACE>.servicebus.windows.net:9093") \
  .option("kafka.sasl.mechanism", "PLAIN") \
  .option("kafka.security.protocol", "SASL_SSL") \
  .option("kafka.sasl.jaas.config", eh_sasl) \
  .option("kafka.request.timeout.ms", "60000") \
  .option("kafka.session.timeout.ms", "30000") \
  .option("kafka.group.id", "$Default") \
  .option("failOnDataLoss", "false") \
  .load()

# COMMAND ----------

display(df)
