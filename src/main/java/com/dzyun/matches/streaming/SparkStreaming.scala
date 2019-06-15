package com.dzyun.matches.streaming

import java.io.File

import com.dzyun.matches.dto.RowEntity
import com.dzyun.matches.hbase.HBaseClient
import com.dzyun.matches.hive.HiveClient
import com.dzyun.matches.util.ShaUtils
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
  private val tableName = "ns:distinct_msg_test"
  private val colName = "file_no"

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
    val dStream = namedTextFileStream(ssc, "hdfs:///home/tiger/origin_data_files/data/")

    def byFileTransformer(filename: String)(rdd: RDD[String]): RDD[(String, String)] =
      rdd.map(line => (filename, line))

    val data = dStream.transform(rdd => transformByFile(rdd, byFileTransformer))
    data.print()
    data.foreachRDD(rdd => {
      rdd.foreach(s => {
        val filename = s._1.split("\\.")(0)
        val line = s._2
        val rowKey = ShaUtils.encrypt(line.split("\t"))
        val puts: java.util.List[RowEntity] = null
        if (!HBaseClient.existsRowKey(tableName, rowKey)) {
          log.info("insert line=" + line)
//          val bean = new RowEntity(rowKey, colName, filename)
//          puts.add(bean)
          //HBaseClient.insert(tableName, rowKey, colName, colName, filename)
        } else {
          log.error("not insert filename=" + filename + " line=" + line)
        }
        if (!puts.isEmpty) {
          HBaseClient.batchAddRow(tableName, colName, puts)
//          HiveClient.add()
        }
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
