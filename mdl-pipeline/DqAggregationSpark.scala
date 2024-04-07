// Databricks notebook source
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder,RowEncoder}
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


case class DQEX(dq: Int, exception : Int)

object DqAggregator extends Aggregator[Row, DQEX, Row] {

  val schema = StructType(Array(
    StructField("dq", IntegerType, true),
    StructField("exception", IntegerType, true)
  ))

  //Initial state of first member = dq_ex value (0,0)
  override def zero: DQEX = DQEX(0,0)

  override def reduce(b: DQEX, a: Row): DQEX = {

    var dq = 0
    var exception = 0

    //println(a.schema)
    var dq_value_a = 0
    var exception_value_a = 0

    if(a != null && a.getStruct(0) != null) {
      dq_value_a = a.getStruct(0).getAs[Int]("dq")
      exception_value_a = a.getStruct(0).getAs[Int]("exception")
    }

    println("In reduce -- dq : " + dq_value_a + " ex : " + exception_value_a + " b_dq : " + b.dq + " b_ex : " + b.exception)

    if(b.dq > dq_value_a){
      dq = b.dq
    } else {
      dq = dq_value_a
    }

    if (b.exception > exception_value_a) {
      exception = b.exception
    } else {
      exception = exception_value_a
    }


    DQEX(dq,exception)
  }

  override def merge(b1: DQEX, b2: DQEX): DQEX = {
    println("In merge ..")

    var dq = 0
    var exception = 0


    if (b1.dq > b2.dq) {
      dq = b1.dq
    } else {
      dq = b2.dq
    }

    if (b1.exception > b2.exception) {
      exception = b1.exception
    } else {
      exception = b2.exception
    }

    DQEX(dq,exception)

  }

  override def finish(reduction: DQEX): Row = {
    println("In finish..")
    Row(reduction.dq, reduction.exception)
  }

  override def bufferEncoder: Encoder[DQEX] = Encoders.product[DQEX]

  override def outputEncoder: Encoder[Row] = RowEncoder.encoderFor(schema)
}

// COMMAND ----------

val service_credential = dbutils.secrets.get(scope="myScope1",key="f1sgdrd-app-client-id-value")
val application_id = "7856f8b6-5ca5-4901-8641-7ddfc62574ee"
val directory_id = "d363c540-a6fe-482e-8941-09d9573b3a2d"

spark.conf.set("fs.azure.account.auth.type.f1sgdrdstg.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1sgdrdstg.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1sgdrdstg.dfs.core.windows.net", application_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1sgdrdstg.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1sgdrdstg.dfs.core.windows.net", s"https://login.microsoftonline.com/$directory_id/oauth2/token")

import org.apache.spark.sql.types.{DataType, DataTypes, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.udaf
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}

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

    val schema2 = StructType(
      Array(
        StructField("", dqStructType, true)
      )
    )

    //Run default on Jan date
    //dbutils.widgets.text("cob_date","2024-01-31")
    val cob_date = dbutils.widgets.get("cob_date")

    spark.udf.register("dqagg", udaf(DqAggregator, RowEncoder.encoderFor(schema2)))





// COMMAND ----------

    val incrDataByAssetClaassDf = spark.sql(s"""select * from f1sgdrd.rd_gd_incr_data_by_asset_class where cob_date = '$cob_date' """)

val incrDqDataByAssetClassDf = spark.sql(s"""select * from f1sgdrd.rd_gd_dq_transposed
where cob_date = '$cob_date' """)
    
    incrDataByAssetClaassDf.join(incrDqDataByAssetClassDf,
      Seq("cob_date","f1_metadata_asset_cd", "mdl_instance_id", "unique_row_key"),
      "left")
      .show(false)


// COMMAND ----------


