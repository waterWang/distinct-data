package com.dzyun.matches.hive;

import java.text.ParseException;
import org.apache.spark.sql.SparkSession;

public class test003 {

  public static void main(String[] args) throws ParseException {
    SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark Hive Example")
        .master("yarn")
        //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
        //.config("hadoop.home.dir", "/user/hive/warehouse")
        .enableHiveSupport()
        .getOrCreate();
    spark.sql("show databases").show();
  }
}
