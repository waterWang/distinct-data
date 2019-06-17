package com.dzyun.matches.streaming

import java.io.File
import java.text.SimpleDateFormat

import com.dzyun.matches.dto.{MsgEntity, RowEntity}
import com.dzyun.matches.hbase.HBaseClient
import com.dzyun.matches.hive.HiveClient
import com.dzyun.matches.util.{DateUtils, ShaUtils, YamlUtil}
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
  private val hdfs_path = YamlUtil.getPatam("hdfsPath")

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
          Some(
            func(filename)(dep.rdd.asInstanceOf[RDD[String]]).setName(filename)
          )
        }
      }.flatten
    )
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("distinct-data").setMaster("yarn")
    val ssc = new StreamingContext(conf, Seconds(3))
    //    val dStream = namedTextFileStream(ssc, "file:///home/tiger/distinct-data/data/") //local file
    val dStream = namedTextFileStream(ssc, hdfs_path)

    def byFileTransformer(filename: String)(rdd: RDD[String]): RDD[(String, String)] =
      rdd.map(line => (filename, line))

    val data = dStream.transform(rdd => transformByFile(rdd, byFileTransformer))
    data.print()
    data.foreachRDD(rdd => {
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
      if (!hbases.isEmpty) {
        HBaseClient.batchAdd(hbases)
        HiveClient.batchAdd(hives)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
