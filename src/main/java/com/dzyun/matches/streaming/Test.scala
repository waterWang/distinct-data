package com.dzyun.matches.streaming

import java.util

import com.dzyun.matches.dto.RowEntity
import com.dzyun.matches.hbase.HBaseClient

object Test {

  def main(args: Array[String]) = {
    val rows: util.List[RowEntity] = new util.ArrayList[RowEntity]
    for (i <- 0 to 10) {
      rows.add(new RowEntity("rowKey" + i, "file_no", "value" + i))
    }
    try {
      HBaseClient.batchAdd(rows)
      System.out.println(HBaseClient.existsRowKey("rowKey5"))
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

}
