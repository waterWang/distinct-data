package com.dzyun.matches.streaming

import java.io.File

import com.dzyun.matches.hbase.JavaHBaseClient
import com.dzyun.matches.util.ShaUtils
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.internal.Logging

import scala.reflect.ClassTag

object SparkStreaming extends Logging {


  //  fileStream produces UnionRDD of NewHadoopRDDs.The good part about NewHadoopRDDs created by sc
  //  .newAPIHadoopFile is that their names are set to their paths

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
    //LogLevelUtils.setLogLevels()
    val conf = new SparkConf().setAppName("Process by file").setMaster("yarn")
    val ssc = new StreamingContext(conf, Seconds(3))
    val dStream = namedTextFileStream(ssc, "file:///home/tiger/spark-in-practice-master/data/")

    def byFileTransformer(filename: String)(rdd: RDD[String]): RDD[(String, String)] =
      rdd.map(line => (filename, line))

    val data = dStream.transform(rdd => transformByFile(rdd, byFileTransformer))
    data.print()
    val tableName = "ns:distinct_msg"
    data.foreachRDD(rdd => {
      rdd.foreach(s => {
        val filename = s._1.split("\\.")(0)
        val line = s._2
        val rowKey = ShaUtils.encrypt(line.split("\t"))
        val hBaseClient = new JavaHBaseClient("10.1.62.109", "2181")
        if (!hBaseClient.existsRowKey(tableName, rowKey)) {
          System.out.println("insert line=" + line)
          log.info("insert line=" + line)
          hBaseClient.insert(tableName, rowKey, "file_no", "file_no", filename)
          //TODO insert hive
        } else {
          System.err.println("not insert filename=" + filename + " line=" + line)
          log.error("not insert filename=" + filename + " line=" + line)
        }
      })

    })
    ssc.start()
    ssc.awaitTermination()
  }

}
