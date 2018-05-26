// Databricks notebook source
// MAGIC %md # Mount Azure Data Lake Store (one-time)

// COMMAND ----------

// MAGIC %md 
// MAGIC - Create Service Principal for Role-based Access using [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/?view=azure-cli-latest)
// MAGIC ```
// MAGIC az ad sp create-for-rbac --name <SERVICE_PRINCIPAL_NAME> --years 5
// MAGIC ```
// MAGIC 
// MAGIC - Assign Service Principal to the ADLS account with permissions (read, write, execute) including all children

// COMMAND ----------

// MAGIC %md ## Store ADLS Secrets (one-time)
// MAGIC [Get application ID, authentication key, and tenant ID](https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-service-to-service-authenticate-using-active-directory#step-2-get-application-id-authentication-key-and-tenant-id)
// MAGIC 
// MAGIC ```
// MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
// MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/scopes/create \
// MAGIC   -d '{"scope": "adls"}'
// MAGIC ```
// MAGIC 
// MAGIC ```
// MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
// MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/secrets/write \
// MAGIC   -d '{"scope": "adls", "key": "clientId", "string_value": "<CLIENT_ID>"}'
// MAGIC ```
// MAGIC 
// MAGIC ```
// MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
// MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/secrets/write \
// MAGIC   -d '{"scope": "adls", "key": "credential", "string_value": "<CREDENTIAL>"}'
// MAGIC ```
// MAGIC 
// MAGIC ```
// MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
// MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/secrets/write \
// MAGIC   -d '{"scope": "adls", "key": "tenantId", "string_value": "<TENANT_ID>"}'
// MAGIC ```
// MAGIC 
// MAGIC ```
// MAGIC curl -H "Authorization: Bearer <AUTH_TOKEN>" -X POST \
// MAGIC   -k https://<CLUSTER_REGION>.azuredatabricks.net/api/2.0/preview/secret/secrets/write \
// MAGIC   -d '{"scope": "adls", "key": "accountName", "string_value": "<ACCOUNT_NAME>"}'
// MAGIC ```

// COMMAND ----------

// MAGIC %md ## [Mount ADLS to DBFS](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-datalake.html)

// COMMAND ----------

def mountADLS(directoryName:String, mountPoint: String) {
  val clientId = dbutils.preview.secret.get(scope = "adls", key = "clientId")
  val credential = dbutils.preview.secret.get(scope = "adls", key = "credential")
  val tenantId = dbutils.preview.secret.get(scope = "adls", key = "tenantId")
  val accountName = dbutils.preview.secret.get(scope = "adls", key = "accountName")
  
  val configs = Map(
    "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
    "dfs.adls.oauth2.client.id" -> clientId,
    "dfs.adls.oauth2.credential" -> credential,
    "dfs.adls.oauth2.refresh.url" -> s"https://login.microsoftonline.com/$tenantId/oauth2/token")
  
  try {   
    dbutils.fs.mount(
      source = s"adl://$accountName.azuredatalakestore.net/$directoryName",
      mountPoint = mountPoint,
      extraConfigs = configs)
  
    println(s"Mounting '$directoryName' to $mountPoint completed!")    
  } catch {
    case e: java.rmi.RemoteException => {
      println(s"$mountPoint is already mounted!")
      dbutils.fs.unmount(mountPoint)
      mountADLS(directoryName, mountPoint)
    }
    case e: Exception => {
      throw e
    }
  }
}

// COMMAND ----------

mountADLS("parquet", "/mnt/parquet")

// COMMAND ----------

// MAGIC %fs mounts

// COMMAND ----------

// MAGIC %fs ls /mnt/parquet

// COMMAND ----------


