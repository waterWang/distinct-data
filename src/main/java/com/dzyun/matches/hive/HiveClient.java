package com.dzyun.matches.hive;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class HiveClient {

  private static SparkSession spark = null;

  static {
    spark = SparkSession
        .builder()
        .appName("Java Spark Hive Example")
        .master("yarn")
        .enableHiveSupport()
        .getOrCreate();
  }

  public static void add(String sql) {
//    Dataset df =spark.sql(sql);
    spark.sql(sql);
  }



}
