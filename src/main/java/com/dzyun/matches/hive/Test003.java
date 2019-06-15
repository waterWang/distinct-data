package com.dzyun.matches.hive;

import com.dzyun.matches.dto.MsgEntity;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Test003 {

  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("Java Spark Hive Example")
        .master("yarn")
        .enableHiveSupport()
        .getOrCreate();
    List<MsgEntity> msgs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      MsgEntity bean = new MsgEntity("861832882276" + i, 156000878L + i,
          "爱又米", "95555", "尊敬的xxx",
          "2019-06-06", "L120190606_123");
      msgs.add(bean);
    }
    Dataset ds = spark.createDataFrame(msgs, MsgEntity.class).toDF("phone_id", "create_time",
        "app_name", "main_call_no", "msg", "the_date", "file_no");
    ds.write().mode("append").format("Hive").option("hive.exec.dynamic.partition.mode","nonstrict").partitionBy("the_date","file_no").saveAsTable("tmp.tmp_msg_www_0630");
    ds.show();
  }
}
