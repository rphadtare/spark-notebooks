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

    val incrDataByAssetClaassDf = List(
      ("2024-02-29", "Lending", 1, "0892", "US_GAAP", "100A-FAPRE-21A", 8050, 100.0, "N"),
      ("2024-02-29", "Lending", 2, "0892", "US_GAAP", "100A-FAPRE-21A", 8050, 50.0, "N"),
      ("2024-02-29", "Lending", 3, "0866", "US_GAAP", "100A-FAPRE-21B", 8050, 100.0, "N"),
      ("2024-02-29", "Listed Derivatives", 1, "0892", "US_GAAP", "101A-FAPRE-21B", 8050, 150.0, "N"),
      ("2024-02-29", "Listed Derivatives", 1, "0850", "US_GAAP", "102A-FAPRE-21B", 8050, 100.0, "N"),
      ("2024-02-29", "Listed Derivatives", 2, "0892", "US_GAAP", "101A-FAPRE-21B", 8050, 250.0, "N"),
      ("2024-02-29", "Listed Derivatives", 2, "0992", "US_GAAP", "100A-FAPRE-21C", 8050, 300.0, "N"),
      ("2024-02-29", "OTC Derivatives", 2, "0866", "US_GAAP", "100A-SAP-21B", 8050, 550.0, "N"),
      ("2024-02-29", "OTC Derivatives", 1, "0866", "US_GAAP", "100A-SAP-21B", 8050, 100.0, "N"),
      ("2024-02-29", "OTC Derivatives", 3, "0866", "US_GAAP", "100A-SAP-21B", 8050, 50.0, "N"),

    ).toDF(
      "cob_date", "f1_metadata_asset_cd", "mdl_instance_id",
      "reporting_entity_cd", "gaap_type", "unique_row_key",
      "reporting_account", "transaction_amt", "deleted_flag"
    )

    incrDataByAssetClaassDf.write.format("delta").mode("overwrite").partitionBy("cob_date").save("abfss://bronze@f1sgdrdstg.dfs.core.windows.net/delta/rd_gd_incr_data_by_asset_class/")

// COMMAND ----------

    import org.apache.spark.sql.types.{DataType, DataTypes, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

    val dqStructType = StructType(Array(
      StructField("dq", IntegerType, true),
      StructField("exception", IntegerType, true)
    ))

    val schema = StructType(
      Array(
        StructField("cob_date", StringType, false),
        StructField("f1_metadata_asset_cd", StringType, false),
        StructField("mdl_instance_id", IntegerType, false),
        StructField("unique_row_key", StringType, false),
        StructField("dqex_gaap_type", dqStructType, true),
        StructField("dqex_transaction_amt", dqStructType, true),
      )
    )

    val cob_date = dbutils.widgets.get("cob_date")

    




// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC
