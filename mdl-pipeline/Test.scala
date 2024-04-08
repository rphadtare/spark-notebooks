// Databricks notebook source
val service_credential = dbutils.secrets.get(scope="myScope1",key="f1sgdrd-app-client-id-value")
val application_id = "7856f8b6-5ca5-4901-8641-7ddfc62574ee"
val directory_id = "d363c540-a6fe-482e-8941-09d9573b3a2d"

spark.conf.set("fs.azure.account.auth.type.f1sgdrdstg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1sgdrdstg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1sgdrdstg.dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1sgdrdstg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1sgdrdstg.dfs.core.windows.net", s"https://login.microsoftonline.com/$directory_id/oauth2/token")

// COMMAND ----------

import io.delta.tables._
import io.delta.sql._
import io.delta.implicits._


// COMMAND ----------

(7 to 100 ).toDF("id").write.format("delta").insertInto("sample") 


// COMMAND ----------

import org.apache.spark.sql.functions._


spark.read.format("delta").option("timestampAsOf", "2024-03-05T14:51:19.000+00:00").load("abfss://stg@f1sgdrdstg.dfs.core.windows.net/delta/student").select("id","age","name").withColumn("postal_cod",lit("8050")).write.mode("overwrite").option("mergeSchema", "true").format("delta").save("abfss://stg@f1sgdrdstg.dfs.core.windows.net/delta/student")

// COMMAND ----------

// MAGIC %sql
// MAGIC create table f1sgdrd.rd_gd_dq_data_by_asset_class 
// MAGIC (
// MAGIC   f1_metadata_asset_cd string,
// MAGIC   mdl_instance_id int,
// MAGIC   
// MAGIC   order_item_price_per_qty int,
// MAGIC   product_id string
// MAGIC )
// MAGIC using delta
// MAGIC partitioned by (order_date date)
// MAGIC location 'abfss://stg@f1sgdrdstg.dfs.core.windows.net/delta/orders/'
// MAGIC

// COMMAND ----------

// MAGIC
// MAGIC %sql
// MAGIC insert overwrite f1sgdrd.orders partition(order_date='2024-03-03')
// MAGIC select order_id, order_item_id, order_item_qty, order_item_price_per_qty, 
// MAGIC   case when order_id between 1 and 3 then 'A-11' 
// MAGIC   when order_id between 3 and 5 then 'A-12'
// MAGIC   else 'A-13' 
// MAGIC   end as 
// MAGIC   product_id
// MAGIC from f1sgdrd.orders where order_date in ('2024-03-04','2024-03-05')
// MAGIC
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

dbutils.notebook.run("ParameterTest",60,Map("order_date" -> "2024-04-03"))

// adding comment for commit