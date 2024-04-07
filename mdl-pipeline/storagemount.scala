// Databricks notebook source
val key = dbutils.secrets.get("myScope1","storage-account-key")


dbutils.fs.mount(
  source = "wasbs://stg@f1sgdrdstg.blob.core.windows.net/",
  mountPoint = "/mnt/stg",
  extraConfigs = Map("fs.azure.account.key.f1sgdrdstg.blob.core.windows.net" -> key)
)




// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://bronze@f1sgdrdstg.blob.core.windows.net/",
  mountPoint = "/mnt/bronze",
  extraConfigs = Map("fs.azure.account.key.f1sgdrdstg.blob.core.windows.net" -> key)
)

dbutils.fs.mount(
  source = "wasbs://silver@f1sgdrdstg.blob.core.windows.net/",
  mountPoint = "/mnt/silver",
  extraConfigs = Map("fs.azure.account.key.f1sgdrdstg.blob.core.windows.net" -> key)
)

dbutils.fs.mount(
  source = "wasbs://gold@f1sgdrdstg.blob.core.windows.net/",
  mountPoint = "/mnt/gold",
  extraConfigs = Map("fs.azure.account.key.f1sgdrdstg.blob.core.windows.net" -> key)
)

// COMMAND ----------

dbutils.fs.unmount("/mnt/stg")
dbutils.fs.unmount("/mnt/bronze")
dbutils.fs.unmount("/mnt/silver")
dbutils.fs.unmount("/mnt/gold")

