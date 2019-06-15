//package com.dzyun.matches.hbase;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.Cell;
//import org.apache.hadoop.hbase.CellUtil;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.*;
//
//import java.io.IOException;
//
//public class TestHbase {
//
//  protected static Connection conn;
//  private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
//  private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
//  //    private static final String HBASE_POS = "10.1.62.6";
//  private static final String ZK_POS = "10.1.62.109:2181";
//  private static final String ZK_PORT_VALUE = "2181";
//
//
//  static {
//    Configuration conf = HBaseConfiguration.create();
////        conf.set("hbase.rootdir", "hdfs://" + HBASE_POS + ":9000/hbase");
//    conf.set(ZK_QUORUM, ZK_POS);
//    conf.set(ZK_CLIENT_PORT, ZK_PORT_VALUE);
//    //创建连接池
//    try {
//      conn = ConnectionFactory.createConnection(conf);
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//  }
//
//  public static void main(String[] args) throws IOException {
//    HTable table = (HTable) conn.getTable(TableName.valueOf("hbase_test"));
//    Scan scan = new Scan();  //  创建扫描对象
//    ResultScanner results = table.getScanner(scan);   //  全表的输出结果
//    for (Result result : results) {
//      for (Cell cell : result.rawCells()) {
//        System.out.println(
//            "\u884c\u952e:" + new String(CellUtil.cloneRow(cell)) + "\t" +
//                "\u5217\u65cf:" + new String(CellUtil.cloneFamily(cell)) + "\t" +
//                "\u5217\u540d:" + new String(CellUtil.cloneQualifier(cell)) + "\t" +
//                "\u503c:" + new String(CellUtil.cloneValue(cell)) + "\t" +
//                "\u65f6\u95f4\u6233:" + cell.getTimestamp());
//      }
//    }
//    System.out.println("11111111");
//    results.close();
//    table.close();
//    conn.close();
//  }
//}