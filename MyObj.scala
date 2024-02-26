// Databricks notebook source
import org.apache.spark.sql.DataFrame

object MyObj {
    def displayData(): DataFrame = {
        return spark.sql("select * from rd_disclosure_data")
    }
}
