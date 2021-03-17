# Databricks notebook source
storage_account_name = "<STORAGE_ACCOUNT_NAME>"
storage_account_key = "<STORAGE_ACCOUNT_KEY>"
blob_container_name = "<BLOB_CONTAINER_NAME>"
dbfs_mount_name = "<DBFS_MOUNT_PATH>"


# COMMAND ----------

dbutils.fs.mount(
  source = f"wasbs://{blob_container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point = f"/mnt/{dbfs_mount_name}",
  extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key})

# COMMAND ----------

# MAGIC %sh ls /dbfs/mnt/

# COMMAND ----------

dfOutput = spark.createDataFrame(
    [
        ("file1.txt", "foo"),
        ("file1.txt", "bar"),
        ("file2.txt", "hello"),
        ("file2.txt", "world"),
    ],
    ["filename", "text"]
)

display(dfOutput)

# COMMAND ----------

import pyspark.sql.functions as f

# Concat all text for the same file with a space " "
dfOutputPerFile = dfOutput \
  .groupby(dfOutput.filename) \
  .agg(f.concat_ws(" ", f.collect_list(dfOutput.text)) \
       .alias("text"))

display(dfOutputPerFile)

# COMMAND ----------

import pathlib

outputDir = f"/dbfs/mnt/{dbfs_mount_name}/output"
pathlib.Path(outputDir).mkdir(parents = True, exist_ok = True)

for row in dfOutputPerFile.collect():
  with open(f"{outputDir}/{row.filename}", "w") as f:
    f.write(row.text)

# COMMAND ----------


