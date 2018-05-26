# Databricks notebook source
# MAGIC %md ## Read from Azure SQL DB

# COMMAND ----------

# MAGIC %md ### Store SQL Server Secrets (one-time)
# MAGIC 
# MAGIC ```
# MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
# MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/scopes/create \
# MAGIC   -d '{"scope": "sqlServer"}'
# MAGIC ```
# MAGIC 
# MAGIC ```
# MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
# MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/secrets/write \
# MAGIC   -d '{"scope": "sqlServer", "key": "server", "string_value": "<DATABASE SERVER WITH PORT>"}'
# MAGIC ```
# MAGIC 
# MAGIC ```
# MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
# MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/secrets/write \
# MAGIC   -d '{"scope": "sqlServer", "key": "database", "string_value": "<DATABASE NAME>"}'
# MAGIC ```
# MAGIC 
# MAGIC ```
# MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
# MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/secrets/write \
# MAGIC   -d '{"scope": "sqlServer", "key": "username", "string_value": "<USERNAME>"}'
# MAGIC ```
# MAGIC 
# MAGIC ```
# MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
# MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/secrets/write \
# MAGIC   -d '{"scope": "sqlServer", "key": "password", "string_value": "<PASSWORD>"}'
# MAGIC ```

# COMMAND ----------

# MAGIC %md ### Create DataFrames

# COMMAND ----------

jdbcHostname = dbutils.preview.secret.get(scope = "sqlServer", key = "server")
jdbcDatabase = dbutils.preview.secret.get(scope = "sqlServer", key = "database")

jdbcUrl = "jdbc:sqlserver://{0};databaseName={1}".format(jdbcHostname, jdbcDatabase)
jdbcUsername = dbutils.preview.secret.get(scope = "sqlServer", key = "username")
jdbcPassword = dbutils.preview.secret.get(scope = "sqlServer", key = "password")

connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df1 = spark.read.jdbc(url=jdbcUrl, table="dbo.table1", properties=connectionProperties)
df2 = spark.read.jdbc(url=jdbcUrl, table="dbo.table2", properties=connectionProperties)

# COMMAND ----------


