// Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.f1sgdrdstg.dfs.core.windows.net",
    dbutils.secrets.get(scope="myScope1", key="storage-account-key"))

dbutils.fs.ls("abfss://stg@f1sgdrdstg.dfs.core.windows.net").foreach(r => println(r.path))    


// COMMAND ----------

// MAGIC %sql 
// MAGIC -- Create table using location
// MAGIC CREATE TABLE f1sgdrd.student (id INT, name STRING, age INT)
// MAGIC USING DELTA
// MAGIC LOCATION "abfss://stg@f1sgdrdstg.dfs.core.windows.net/delta/student";
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into f1sgdrd.student
// MAGIC select 1 as id, "Rohit" as name, 31 as age union 
// MAGIC select 2 as id, "Raj" as name, 31 as age union 
// MAGIC select 3 as id, "Ramesh" as name, 32 as age;

// COMMAND ----------

// MAGIC %sql
// MAGIC update f1sgdrd.student set id = 4 where id = 3;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from f1sgdrd.student version as of 1 union 
// MAGIC select * from f1sgdrd.student;

// COMMAND ----------

spark.read.format("delta").load("abfss://stg@f1sgdrdstg.dfs.core.windows.net/delta/student").show
