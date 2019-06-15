package com.dzyun.matches.hive;

import com.dzyun.matches.dto.MsgEntity;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class HiveClient {

  private static SparkSession spark;
//  private static Seq<String> cols = JavaConverters
//      .asScalaIteratorConverter(Arrays.asList("phone_id", "create_time",
//          "app_name", "main_call_no", "msg", "the_date", "file_no").iterator()).asScala().toSeq();
private static Seq<String> cols = JavaConverters
    .asScalaIteratorConverter(Arrays.asList("app_name", "create_time",
        "file_no", "main_call_no", "msg", "phone_id", "the_date").iterator()).asScala().toSeq();

  private static String tableName = "tmp.tmp_msg_www_0630";

  static {
    spark = SparkSession
        .builder()
        .appName("Java Spark Hive Example")
        .master("yarn")
        .enableHiveSupport()
        .getOrCreate();
    spark.sqlContext().setConf("hive.exec.dynamic.partition", "true");
    spark.sqlContext().setConf("hive.exec.dynamic.partition.mode", "nonstrict");
  }

  public static void batchAdd(List<MsgEntity> msgs) {

    Dataset ds = spark.createDataFrame(msgs, MsgEntity.class).toDF(cols);
    ds.write().mode("append").format("Hive").partitionBy("the_date", "file_no")
        .saveAsTable(tableName);
    ds.show();
  }

  public static void main(String[] args) {
    List<MsgEntity> msgs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      MsgEntity bean = new MsgEntity("861832882276" + i, 156000878L + i,
          "爱又米", "95555", "尊敬的xxx",
          "2019-06-06", "L120190606_123");
      msgs.add(bean);
    }
    batchAdd(msgs);
  }


}
