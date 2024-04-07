// Databricks notebook source
import io.delta.tables._
import io.delta.sql._
import io.delta.implicits._

val key = dbutils.secrets.get("myScope1","storage-account-key")

DeltaTable.createOrReplace(spark)
  .tableName("f1sgdrd.people")
  .addColumn("id", "INT")
  .addColumn("firstName", "STRING")
  .addColumn("middleName", "STRING")
  .addColumn("lastName", "STRING")
  .addColumn("gender", "STRING")
  .addColumn("birthDate", "TIMESTAMP")
  .addColumn("ssn", "STRING")
  .addColumn("salary", "INT")
  .property("description", "table with people data")
  .location("/mnt/stg/delta/people")
  .execute()

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE f1sgdrd.student (id INT, name STRING, age INT)
// MAGIC USING DELTA
// MAGIC LOCATION "/mnt/stg/delta/student";

// COMMAND ----------

val service_credential = dbutils.secrets.get(scope="myScope1",key="f1sgdrd-app-client-id-value")
val application_id = "7856f8b6-5ca5-4901-8641-7ddfc62574ee"
val directory_id = "d363c540-a6fe-482e-8941-09d9573b3a2d"

spark.conf.set("fs.azure.account.auth.type.f1sgdrdstg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1sgdrdstg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1sgdrdstg.dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1sgdrdstg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1sgdrdstg.dfs.core.windows.net", s"https://login.microsoftonline.com/$directory_id/oauth2/token")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from f1sgdrd.student;
