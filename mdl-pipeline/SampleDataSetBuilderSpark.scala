// Databricks notebook source

spark.conf.set("fs.azure.account.auth.type.f1sgdrdstg.dfs.core.windows.net", "SAS")
	    spark.conf.set("fs.azure.sas.token.provider.type.f1sgdrdstg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
                                 spark.conf.set("fs.azure.sas.fixed.token.f1sgdrdstg.dfs.core.windows.net", dbutils.secrets.get(scope="myScope1", key="storage-account-sas-token"))


// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select * from f1sgdrd.student;
