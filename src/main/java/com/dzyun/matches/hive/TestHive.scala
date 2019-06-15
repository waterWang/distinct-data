package com.dzyun.matches.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object TestHive {

  def main(args: Array[String]): Unit = {


//    val conf = Configra
//    val sparkSession = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate

    val spark =
      SparkSession.builder().enableHiveSupport().config("hive.exec.dynamic.partition",
        "true").config("hive.exec.dynamic.partition.mode",
        "nonstrict").getOrCreate()


//    val rddDF = spark.createDataFrame(rdd, StructType(schema))
//    rddDF.createOrReplaceTempView("test")
    val sql = "show databases;"
    spark.sql(sql).show()
  }
}
