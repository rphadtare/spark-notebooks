// Databricks notebook source
val service_credential = dbutils.secrets.get(scope="myScope1",key="f1sgdrd-app-client-id-value")
val application_id = "7856f8b6-5ca5-4901-8641-7ddfc62574ee"
val directory_id = "d363c540-a6fe-482e-8941-09d9573b3a2d"

spark.conf.set("fs.azure.account.auth.type.f1sgdrdstg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1sgdrdstg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1sgdrdstg.dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1sgdrdstg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1sgdrdstg.dfs.core.windows.net", s"https://login.microsoftonline.com/$directory_id/oauth2/token")

import io.delta.tables._
import io.delta.sql._
import io.delta.implicits._

// COMMAND ----------

val order_date = dbutils.widgets.get("order_date")
dbutils.notebook.exit(order_date)
