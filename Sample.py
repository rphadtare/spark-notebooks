# Databricks notebook source
# MAGIC %scala
# MAGIC val df = List(("2023-03-31","100-uaq-001","Listed Derivaitves",1,1010,100),
# MAGIC   ("2023-03-31","100-uaq-002","Listed Derivaitves",1,1010,100),
# MAGIC   ("2023-03-31","100-uaq-003","Listed Derivaitves",1,1098,100),
# MAGIC   ("2023-03-31","100-uaq-004","Listed Derivaitves",1,1080,100),
# MAGIC   ("2023-03-31","100-uaq-005","Lending",1,1010,100),
# MAGIC   ("2023-03-31","100-uaq-006","Lending",1,1022,100),
# MAGIC   ("2023-03-31","100-uaq-007","Lending",1,1023,100),
# MAGIC ).toDF("cob_date","unique_row_key","asset_cd","mdl_instance_id","reporting_account","amt")
# MAGIC
# MAGIC display(df)
# MAGIC
# MAGIC df.write.mode("overwrite").partitionBy("cob_date").saveAsTable("rd_disclosure_data")

# COMMAND ----------

# MAGIC %scala
# MAGIC val df1 = List(("2023-03-31","100-uaq-001","Listed Derivaitves",1,1010,101),
# MAGIC   ("2023-03-31","100-uaq-002","Listed Derivaitves",1,1010,101)
# MAGIC ).toDF("cob_date","unique_row_key","asset_cd","mdl_instance_id","reporting_account","amt")
# MAGIC
# MAGIC display(df1)
# MAGIC
# MAGIC df1.write.mode("append").insertInto("rd_disclosure_data")

# COMMAND ----------

# MAGIC %scala
# MAGIC val df1 = List(("2024-03-31","101-uaq-001","Listed Derivaitves",1,1010,101),
# MAGIC   ("2024-03-31","101-uaq-002","Listed Derivaitves",1,1010,101)
# MAGIC ).toDF("cob_date","unique_row_key","asset_cd","mdl_instance_id","reporting_account","amt")
# MAGIC
# MAGIC display(df1)
# MAGIC
# MAGIC df1.write.option("partitionOverwriteMode","dynamic").insertInto("rd_disclosure_data")
