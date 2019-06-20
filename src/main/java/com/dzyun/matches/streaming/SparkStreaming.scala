package com.dzyun.matches.streaming

import java.io.File
import java.util

import com.dzyun.matches.hbase.HBaseClient
import com.dzyun.matches.util.{ShaUtils, StringUtils}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.ClassTag
import scala.util.Success

object SparkStreaming extends java.io.Serializable {

  val log: Logger = LoggerFactory.getLogger(SparkStreaming.getClass)
  val colName = "file_no"
  val line_regex = "\t"
  val file_name_regex = "\\."
  //  private val hdfs_path = YamlUtil.getPatam("hdfsPath")
  val base_dir = "hdfs:///user/tiger/org_txt_distinct/"
  val file_dir = base_dir + "origin_data_files_test/"
  val filter_dir = base_dir + "filter_files/"
  val checkpoint_dir = base_dir + "checkpoint"
  //"file:///home/tiger/distinct-data/data/"

  def createContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("org_txt_distinct").setMaster("yarn")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint(checkpoint_dir)
    ssc.sparkContext.setLogLevel("WARN")
    ssc
  }

  def txtFileStream(ssc: StreamingContext, dir: String): DStream[String] =
    ssc.fileStream[LongWritable, Text, TextInputFormat](dir)
      .transform(rdd =>
        new UnionRDD(rdd.context, rdd.dependencies.map(dep =>
          dep.rdd.asInstanceOf[RDD[(LongWritable, Text)]].map(_._2.toString).setName(dep.rdd.name))
        )
      )


  def transformByFile[U: ClassTag](rdd: RDD[String], func: String => RDD[String] => RDD[U]): RDD[U] = {
    new UnionRDD(rdd.context,
      rdd.dependencies.flatMap { dep =>
        if (dep.rdd.isEmpty) None
        else {
          val path = dep.rdd.name
          if (path.endsWith(".tmp")) None
          else {
            val fileName = new File(path).getName.split(".txt")(0)
            Some(func(fileName)(dep.rdd.asInstanceOf[RDD[String]]).setName(fileName))
          }
        }
      }
    )
  }

  def byFileTransformer(filename: String)(rdd: RDD[String]): RDD[(String, String)] =
    rdd.map(line => (filename, line))

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate(checkpoint_dir, createContext)
    val dStream = txtFileStream(ssc, file_dir)
    val data = dStream.transform(rdd => transformByFile(rdd, byFileTransformer))

    if (null != data) {
      //      data.foreachRDD(rdd => {
      //        log.warn("===start streaming===")
      //        val start = System.currentTimeMillis()
      //        val hives: java.util.List[MsgEntity] = new util.ArrayList[MsgEntity]()
      //        val hbases: java.util.List[String] = new util.ArrayList[String]()
      //        rdd.take(3).foreach(println)
      //        val cnt = rdd.count()
      //        //        var fileName: String = null
      //        //        var row: String = null
      //        //        var rowKey: String = null
      //        //        var arr: Array[String] = null
      //        rdd.foreachPartition(tuple => {
      //          val fileName = tuple.next()._1
      //          var ss: (String, String) = null
      //          while (tuple.hasNext) {
      //            ss = tuple.next()
      //            //            val fileName = ss._1
      //            val row = ss._2
      //            val arr = row.split(line_regex)
      //            if (arr.length >= 5) {
      //              val rowKey = ShaUtils.encrypt(arr(0), arr(1), arr(3), arr(4))
      //              if (!HBaseClient.existsRowKey(rowKey)) {
      //                val hiveBean = new MsgEntity()
      //                hiveBean.setPhone_id(arr(0))
      //                hiveBean.setCreate_time(str2Long(arr(1)))
      //                hiveBean.setApp_name(arr(2))
      //                hiveBean.setMain_call_no(arr(3))
      //                hiveBean.setMsg(arr(4))
      //                hiveBean.setThe_date(DateUtils.strToDateFormat(fileName.split("_")(0).substring(2)))
      //                hiveBean.setFile_no(fileName)
      //
      //                hives.add(hiveBean)
      //                hbases.add(rowKey)
      //              } else {
      //                log.warn("not insert fileName=" + fileName + " row=" + row)
      //              }
      //            }
      //          }
      //          if (!hbases.isEmpty) {
      //            log.warn("===start batch insert,cnt is {}", hbases.size())
      //            HBaseClient.batchAdd(hbases, fileName)
      //            HiveClient.batchAdd(hives)
      //          }
      //        })
      //        val cost = System.currentTimeMillis() - start
      //        log.warn("===end streaming,cost time is {}s,row cnt is {}", cost / 1000, cnt)
      //      })

      data.filter(ss => { // 满足条件的保留
        val row = ss._2
        val arr = row.split(line_regex)
        var isNotExist = false
        if (arr.length >= 5) {
          val rowKey = ShaUtils.encrypt(arr(0), arr(1), arr(3), arr(4))
          if (!HBaseClient.existsRowKey(rowKey)) {
            isNotExist = true
          } else {
            //ambiguous reference to overloaded definition
            //log.warn("===cannot insert fileName is {},row is {}", ss._1, row)
            System.err.println("===cannot insert fileName is " + ss._1 + ",row is " + row)
          }
        }
        isNotExist
      }).foreachRDD(rdd => {
        val fileName = rdd.take(1).apply(1)._1
        val the_date = StringUtils.fileName2TheDate(fileName)
        val path = file_dir + the_date + "/" + fileName + "/"
        rdd.map(ss => ss._2).repartition(1).saveAsTextFile(path)
        val hbases: java.util.List[String] = new util.ArrayList[String]()
        rdd.foreach(ss => {
          val row = ss._2
          val arr = row.split(line_regex)
          val rowKey = ShaUtils.encrypt(arr(0), arr(1), arr(3), arr(4))
          hbases.add(rowKey)
        })
        HBaseClient.batchAdd(hbases, fileName)
      })
    }
    ssc.start()
    ssc.awaitTermination()
  }


  def str2Long(s: String): Long = {
    val r1 = scala.util.Try(s.toLong)
    r1 match {
      case Success(_) => s.toLong;
      case _ => -1L
    }
  }

}
