package com.dzyun.matches.hive;

import com.dzyun.matches.dto.MsgEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

public class HiveClient implements Serializable {

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
        .appName("Java Spark Hive")
        .master("yarn")
        .enableHiveSupport()
        .getOrCreate();
    spark.sqlContext().sql("set hive.exec.dynamic.partition=true");
    spark.sqlContext().sql("set hive.exec.dynamic.partition.mode=nonstrict");
    spark.sqlContext().sparkContext().setLogLevel("warn");
//    spark.sqlContext().sql("insert overwrite table hive_test.query_result_info  partition(dt) " +
//        "select query, code, info, $dt " +
//        "from queryResultTempTable ");
  }

  public static void batchAdd(List<MsgEntity> msgs) {
    log.warn("start insert hive===" + msgs.size());
    Long start = System.currentTimeMillis();
    Dataset<Row> ds = spark.createDataFrame(msgs, MsgEntity.class).toDF(cols);
    ds.repartition(1).write().mode("append").format("Hive").partitionBy("the_date", "file_no")
        .saveAsTable(tableName);
    Long cost = (System.currentTimeMillis() - start) / 1000;
    log.warn("===insert hive cnt is {},cost time is {}s", msgs.size(), cost);
  }


//  public static void write2hdfs(Object listContent, String filePath) throws IOException {
//    ObjectMapper objectMapper = new ObjectMapper();
//    Path path = new Path(filePath);
//    FileSystem fs = path.getFileSystem(conf);
//    if (!fs.exists(path)) {
//      fs.createNewFile(path);
//    }
//    FSDataOutputStream output = fs.append(new Path(filePath));
//    System.out.println(listContent.toString());
//    output.write(objectMapper.writeValueAsString(listContent).getBytes("UTF-8"));
//    output.write("\n".getBytes("UTF-8"));//换行
//    fs.close();
//    output.close();
//  }

//  public static void writeHdfs(List<MsgEntity> msgs) {
//    spark.sqlContext().sparkContext().makeRDD(msgs,1,MsgEntity.class);
//    log.warn("start insert hive===" + msgs.size());
//    Long start = System.currentTimeMillis();
//    Dataset<Row> ds = spark.createDataFrame(msgs, MsgEntity.class).toDF(cols);
//    ds.write().mode("append").format("Hive").partitionBy("the_date", "file_no")
//        .saveAsTable(tableName);
//    Long cost = (System.currentTimeMillis() - start) / 1000;
//    log.warn("===insert hive cnt is {},cost time is {}s", msgs.size(), cost);
//  }

  public static void main(String[] args) {
    Long len = Long.parseLong(args[0]);
    List<MsgEntity> msgs = new ArrayList<>();
    for (int i = 0; i < len; i++) {
      MsgEntity bean = new MsgEntity("861832882276" + i, 156000878L + i,
          "爱又米", "95555", "尊敬的xxx",
          "2019-06-06", "L120190606_123");
      msgs.add(bean);
    }
    batchAdd(msgs);
  }


}
