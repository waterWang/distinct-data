package com.dzyun.matches.hive;

import com.dzyun.matches.dto.MsgEntity;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class HiveClient {

  private static final Logger log = LoggerFactory.getLogger(HiveClient.class);
  private static SparkSession spark;


  //按照字段顺序
  private static Seq<String> cols = JavaConverters
      .asScalaIteratorConverter(Arrays.asList("app_name", "create_time",
          "file_no", "main_call_no", "msg", "phone_id", "the_date").iterator()).asScala().toSeq();

  //  private static String tableName = YamlUtil.getPatam("hiveTableName");
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
    log.warn("start insert hive===" + msgs.size());
    Dataset<Row> ds = spark.createDataFrame(msgs, MsgEntity.class).toDF(cols);
    ds.write().mode("append").format("Hive").partitionBy("the_date", "file_no")
        .saveAsTable(tableName);
  }

//  public static void main(String[] args) {
//    List<MsgEntity> msgs = new ArrayList<>();
//    for (int i = 0; i < 10; i++) {
//      MsgEntity bean = new MsgEntity("861832882276" + i, 156000878L + i,
//          "爱又米", "95555", "尊敬的xxx",
//          "2019-06-06", "L120190606_123");
//      msgs.add(bean);
//    }
//    batchAdd(msgs);
//  }


}
