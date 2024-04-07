// Databricks notebook source
// MAGIC %sql 
// MAGIC select * from f1sgdrd.rd_gd_notifications

// COMMAND ----------

case class notifications(id:Int, cob_date:String, status:String)
val list_open_notification = spark.sql("select cob_date from f1sgdrd.rd_gd_notifications where Status = 'Open' ").
    rdd.collect.toList.mkString(",").replaceAll("]","").replaceAll("\\[","")
    //map(r => notifications(r.getInt(0), r.getString(1), r.getString(2)))


dbutils.notebook.exit(list_open_notification)    

