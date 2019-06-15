//package com.dzyun.matches.hbase
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.client._
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
//import org.apache.spark.internal.Logging
//
///**
//  * 从hbase中增删改查数据
//  *
//  */
//object HbaseClient extends Logging {
//
//
//  def getHbaseConf: Configuration = {
//    val conf: Configuration = HBaseConfiguration.create
//    conf.set("hbase.zookeeper.property.clientPort", "2181")
//    conf.set("hbase.zookeeper.quorum", "10.1.62.109")
//    //    conf.set("spark.executor.memory", "3000m")
//    //    conf.set("hbase.master", "10.1.62.69")
//    //    conf.set("hbase.rootdir", "hdfs://quickstart.cloudera:8020/hbase")
//    conf
//  }
//
//
//  def createTable(conf: Configuration, hBaseConn: Connection, tablename: String, columnFamily: Array[String]) {
//    val admin: HBaseAdmin = hBaseConn.getAdmin.asInstanceOf[HBaseAdmin]
//
//    if (admin.tableExists(tablename)) {
//      log.info(tablename + " Table exists!")
//      val tableDesc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tablename))
//      tableDesc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation")
//    }
//    else {
//      val tableDesc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tablename))
//      tableDesc.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation")
//      for (i <- 0 to (columnFamily.length - 1)) {
//        val columnDesc: HColumnDescriptor = new HColumnDescriptor(columnFamily(i));
//        tableDesc.addFamily(columnDesc);
//      }
//      admin.createTable(tableDesc)
//      log.info(tablename + " create table success!")
//    }
//    admin.close
//  }
//
//
//  def addRow(table: Table, rowKey: String, columnFamily: String, key: String, value: String) {
//    log.info("put '" + rowKey + "', '" + columnFamily + ":" + key + "', '" + value + "'")
//
//    val rowPut: Put = new Put(Bytes.toBytes(rowKey))
//    if (value == null) {
//      rowPut.addColumn(columnFamily.getBytes, key.getBytes, "".getBytes)
//    } else {
//      rowPut.addColumn(columnFamily.getBytes, key.getBytes, value.getBytes)
//    }
//    table.put(rowPut)
//  }
//
//
//  def putRow(rowPut: Put, table: HTable, rowKey: String, columnFamily: String, key: String, value: String) {
//    val rowPut: Put = new Put(Bytes.toBytes(rowKey))
//    println("put '" + rowKey + "', '" + columnFamily + ":" + key + "', '" + value + "'")
//    if (value == null) {
//      rowPut.add(columnFamily.getBytes, key.getBytes, "".getBytes)
//    } else {
//      rowPut.add(columnFamily.getBytes, key.getBytes, value.getBytes)
//    }
//    table.put(rowPut)
//
//  }
//
//  def getRow(table: HTable, rowKey: String): Result = {
//    val get: Get = new Get(Bytes.toBytes(rowKey))
//    val result: Result = table.get(get)
//    return result
//  }
//
//
//
//  def dropTable(conf: HBaseConfiguration, tableName: String) {
//    try {
//      val admin: HBaseAdmin = new HBaseAdmin(conf)
//      admin.disableTable(tableName)
//      admin.deleteTable(tableName)
//    }
//    catch {
//      case e: Exception => {
//        log.error(e.toString)
//      }
//    }
//  }
//}
