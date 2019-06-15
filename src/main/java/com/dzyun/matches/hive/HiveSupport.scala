package com.dzyun.matches.hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 通过spark操作hive  把hive.site.xml放到resources中即可把元数据信息写入配置的mysql中
  */
object HiveSupport {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hive_test").setMaster("spark://quickstart.cloudera:7077")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")
    //操作hive
    //    sparkSession.sql("create table if not exists person(id int,name string,age int) row format delimited fields terminated by ','")

    //    sparkSession.sql("load data local inpath './data/person.txt' into table person")
    spark.sql("show databases").show()
//    spark.stop()
  }
}
