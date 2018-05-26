// Databricks notebook source
// MAGIC %md # Mount Azure Storage (one-time)

// COMMAND ----------

// MAGIC %md ## Store Blob Storage Secrets (one-time)
// MAGIC 
// MAGIC ```
// MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
// MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/scopes/create \
// MAGIC   -d '{"scope": "azureStorage"}'
// MAGIC ```
// MAGIC 
// MAGIC ```
// MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
// MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/secrets/write \
// MAGIC   -d '{"scope": "azureStorage", "key": "accountName", "string_value": "<ACCOUNT_NAME>"}'
// MAGIC ```
// MAGIC 
// MAGIC ```
// MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
// MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/secrets/write \
// MAGIC   -d '{"scope": "azureStorage", "key": "accessKey", "string_value": "<ACCESS_KEY>"}'
// MAGIC ```

// COMMAND ----------

// MAGIC %md ## [Mount Blob Storage to DBFS](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-storage.html)

// COMMAND ----------

def mountAzureStorage(containerName:String, mountPoint: String) {
  val accountName = dbutils.preview.secret.get(scope = "azureStorage", key = "accountName")
  val accessKey = dbutils.preview.secret.get(scope = "azureStorage", key = "accessKey")
  
  try {
    dbutils.fs.mount(
      source = s"wasbs://$containerName@$accountName.blob.core.windows.net/",
      mountPoint = mountPoint,
      extraConfigs = Map(s"fs.azure.account.key.$accountName.blob.core.windows.net" -> accessKey))
  
    println(s"Mounting '$containerName' to $mountPoint completed!")    
  } catch {
    case e: java.rmi.RemoteException => {
      println(s"$mountPoint is already mounted!")
      dbutils.fs.unmount(mountPoint)
      mountAzureStorage(containerName, mountPoint)
    }
    case e: Exception => {
      throw e
    }
  }
}

// COMMAND ----------

mountAzureStorage("parquet", "/mnt/parquet")

// COMMAND ----------

// MAGIC %fs mounts

// COMMAND ----------

// MAGIC %fs ls /mnt/parquet
