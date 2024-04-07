// Databricks notebook source
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
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

  override def outputEncoder: Encoder[Row] = ExpressionEncoder()
}
