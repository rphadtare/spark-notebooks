// Databricks notebook source
import spark.implicits._

dbutils.widgets.text("my_parameter", "rd_disclosure_data")

val table_name = dbutils.widgets.get("my_parameter")

val df1 = spark.read.table(s"$table_name").filter("cob_date == '2023-03-31'")
display(df1)

// COMMAND ----------


val df2 = spark.read.table("rd_disclosure_data").filter("cob_date == '2024-03-31'")
display(df2)

// COMMAND ----------

println(MyObj.displayData)
