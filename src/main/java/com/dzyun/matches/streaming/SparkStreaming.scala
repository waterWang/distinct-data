package com.dzyun.matches.streaming

import java.io.File

import com.dzyun.matches.dto.{MsgEntity, RowEntity}
import com.dzyun.matches.hbase.HBaseClient
import com.dzyun.matches.hive.HiveClient
import com.dzyun.matches.util.{DateUtils, ShaUtils}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

object SparkStreaming {

  private val log = LoggerFactory.getLogger(SparkStreaming.getClass)
  private val colName = "file_no"
  private val line_regex = "\t"
  private val file_name_regex = "\\."
  //  private val hdfs_path = YamlUtil.getPatam("hdfsPath")
  private val file_dir = "hdfs:///user/tiger/origin_data_files_test/"
  private val checkpoint_dir = "hdfs:///user/tiger/test"
  //  private val file_dir = "file:///home/tiger/distinct-data/data/"

  /**
    * https://spark.apache.org/docs/2.2.0/streaming-programming-guide.html#checkpointing
    * Create spark streamingContext function for getOrCreate method
    */
  def createContext(): StreamingContext = {

    val conf = new SparkConf().setAppName("distinct-data").setMaster("yarn")
    val ssc = new StreamingContext(conf, Seconds(3))
    ssc.checkpoint(checkpoint_dir)
    ssc.sparkContext.setLogLevel("WARN")
    ssc
  }

  def namedTextFileStream(ssc: StreamingContext, dir: String): DStream[String] =

    ssc.fileStream[LongWritable, Text, TextInputFormat](dir)
      .transform(rdd =>
        new UnionRDD(rdd.context, rdd.dependencies.map(dep =>
          dep.rdd.asInstanceOf[RDD[(LongWritable, Text)]].map(_._2.toString).setName(dep.rdd.name))
        )
      )



  def transformByFile[U: ClassTag](rdd: RDD[String], func: String => RDD[String] => RDD[U]): RDD[U] = {
    new UnionRDD(rdd.context,
      rdd.dependencies.map { dep =>
        if (dep.rdd.isEmpty) None
        else {
          val filename = new File(dep.rdd.name).getName
          if (filename.endsWith(".tmp")) None
          else {
            Some(
              func(filename)(dep.rdd.asInstanceOf[RDD[String]]).setName(filename)
            )
          }

        }
      }.flatten
    )
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("distinct-data").setMaster("yarn")
    val ssc = new StreamingContext(conf, Seconds(3))

    //    val ssc = StreamingContext.getOrCreate(checkpoint_dir, createContext _)
    val dStream = namedTextFileStream(ssc, file_dir)

    def byFileTransformer(filename: String)(rdd: RDD[String]): RDD[(String, String)] =
      rdd.map(line => (filename, line))

    val data = dStream.transform(rdd => transformByFile(rdd, byFileTransformer))
    log.info("=============start streaming==============")
    val start = System.currentTimeMillis()
    if (null != data) {
      data.foreachRDD(rdd => {
        rdd.take(10).foreach(println)
        val hives: java.util.List[MsgEntity] = null
        val hbases: java.util.List[RowEntity] = null
        rdd.foreach(s => {
          val filename = s._1.split(file_name_regex)(0)
          val line = s._2
          val arr = line.split(line_regex)
          val rowKey = ShaUtils.encrypt(arr(0), arr(1), arr(3), arr(4))
          if (!HBaseClient.existsRowKey(rowKey)) {
            log.info("insert line=" + line)
            val hiveBean = new MsgEntity()

            hiveBean.setPhone_id(arr(0))
            hiveBean.setCreate_time(arr(1).toLong)
            hiveBean.setApp_name(arr(2))
            hiveBean.setMain_call_no(arr(3))
            hiveBean.setMsg(arr(4))
            hiveBean.setThe_date(DateUtils.format(arr(1)))
            hiveBean.setFile_no(filename)

            val hbaseBean = new RowEntity()
            hbaseBean.setRowKey(rowKey)
            hbaseBean.setCol(colName)
            hbaseBean.setValue(filename)
            hives.add(hiveBean)
            hbases.add(hbaseBean)
          } else {
            log.error("not insert filename=" + filename + " line=" + line)
          }
        })
        if (null != hbases && !hbases.isEmpty) {
          HBaseClient.batchAdd(hbases)
          HiveClient.batchAdd(hives)
        }
      })
    }
    val cost = System.currentTimeMillis() - start
    log.info("=============end streaming,cost time is==============" + cost / 1000)
    ssc.start()
    ssc.awaitTermination()
  }

}
