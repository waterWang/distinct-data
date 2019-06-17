package com.dzyun.matches.hbase;


import com.dzyun.matches.util.ConstUtil;
import com.dzyun.matches.dto.RowEntity;
import com.dzyun.matches.util.LabelResult;
import com.dzyun.matches.util.MsgException;
import com.dzyun.matches.util.ResultFormatter;
import com.dzyun.matches.util.YamlUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hbase 通用接口
 */
public class HBaseClient {

  private static final Logger log = LoggerFactory.getLogger(HBaseClient.class);

  private static final String QUORUM = "dz-prod-dc1-hadoop1,dz-prod-dc1-hadoop2,dz-prod-dc1-hadoop3";
//  private static final String QUORUM = YamlUtil.getPatam("quorum");
//  private static final String CLIENT_PORT = YamlUtil.getPatam("hbasePort");
  private static final String CLIENT_PORT = "2181";
  private static Configuration conf = null;
  private static Connection conn = null;
  private static Admin admin = null;
//  private static String tableName = YamlUtil.getPatam("hbaseTableName");
  private static String tableName = "ns:distinct_msg_test";
  private static String colName = "file_no";


  static {
    try {
      conf = HBaseConfiguration.create();
      conf.set(ConstUtil.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, CLIENT_PORT);
      conf.set(ConstUtil.HBASE_ZOOKEEPER_QUORUM, QUORUM);
      conn = ConnectionFactory.createConnection(conf);
      admin = conn.getAdmin();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  public static void main(String[] args) {

    List<RowEntity> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(new RowEntity("rowKey" + i, colName, "value" + i));
    }
    try {
      batchAdd(rows);
      System.out.println(existsRowKey("rowKey5"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 功能描述：关闭连接
   *
   * @author qiaobin
   */
  public static void close() {
    try {
      if (null != admin) {
        admin.close();
      }
      if (null != conn) {
        conn.close();
      }
    } catch (IOException e) {
      throw new MsgException("关闭失败", e);
    }

  }

  /**
   * 创建HBase表
   *
   * @param table 指定表名
   * @param columnFamilies 初始化的列簇，可以指定多个
   */
  public void createTable(String table, Collection<String> columnFamilies) {
    TableName tableName = TableName.valueOf(table);
    try {
      if (admin.tableExists(tableName)) {
        throw new MsgException("该表已存在");
      } else {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        for (String col : columnFamilies) {
          HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
          hColumnDescriptor.setMaxVersions(ConstUtil.MAX_VERSION);
          hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
      }
    } catch (IOException e) {
      throw new MsgException("建表失败", e);
    }
  }

  /**
   * 创建HBase表
   *
   * @param table 指定表名
   * @param colFamily 初始化的列簇，可以指定多个
   */
  public void createTable(String table, String colFamily) {
    if (!StringUtils.isNotEmpty(colFamily)) {
      colFamily = ConstUtil.COLUMNFAMILY_DEFAULT;
    }
    TableName tableName = TableName.valueOf(table);
    try {
      if (admin.tableExists(tableName)) {
        throw new MsgException("该表已存在");
      } else {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
        hColumnDescriptor.setMaxVersions(ConstUtil.MAX_VERSION);
        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);
      }
    } catch (IOException e) {
      throw new MsgException("建表失败", e);
    }
  }

  /**
   * 删除HBase表
   *
   * @param table 表名
   */
  public void dropTable(String table) {
    TableName tableName = TableName.valueOf(table);
    try {
      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }
    } catch (IOException e) {
      throw new MsgException("删除表失败", e);
    }
  }


  /**
   * 禁用HBase表
   *
   * @param table 表名
   */
  public void disableTable(String table) {
    TableName tableName = TableName.valueOf(table);
    try {
      if (admin.tableExists(tableName)) {
        admin.disableTable(tableName);
      }
    } catch (IOException e) {
      throw new MsgException("关闭表失败", e);
    }
  }


  /**
   * 更新表名
   *
   * @param oldTableName 原表名
   * @param newTableName 新表名
   */
  public void changeTableName(String oldTableName, String newTableName) {
    TableName oldTable = TableName.valueOf(oldTableName);
    TableName newTable = TableName.valueOf(newTableName);
    try {
      Admin admin = conn.getAdmin();
      String snapshotName = "snapshot";
      admin.disableTable(oldTable);
      admin.snapshot(snapshotName, oldTable);
      admin.cloneSnapshot(snapshotName, newTable);
      admin.deleteSnapshot(snapshotName);
      admin.deleteTable(oldTable);
    } catch (Exception e) {
      throw new MsgException("表名修改失败", e);
    }
  }

  /**
   * 功能描述：返回所有表名
   */
  public List<String> listTableNames() {
    List<String> list = new ArrayList<>();
    try {
      HTableDescriptor hTableDescriptors[] = admin.listTables();
      for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
        list.add(hTableDescriptor.getNameAsString());
      }
    } catch (IOException e) {
      throw new MsgException("获取表名列表失败", e);
    }
    return list;
  }

  /**
   * 给指定的表添加数据
   *
   * @param tableName 表名
   * @param rowKey 行健
   * @param colFamily 列族
   * @param col 字段
   * @param value 值
   */
  public static void insert(String tableName, String rowKey, String colFamily, String col,
      String value) throws IOException {
    TableName tablename = TableName.valueOf(tableName);
    Table table = conn.getTable(tablename);
    Put put = new Put(Bytes.toBytes(rowKey));
    put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(value));
    table.put(put);
  }

  /**
   * 给指定的表添加数据
   *
   * @param tableName 表名
   * @param rowKey 行健
   * @param colFamily 列族
   * @param colValue 字段-值
   */
  public void insertRow(String tableName, String rowKey, String colFamily,
      Map<String, Object> colValue, long timestamp) throws IOException {
    if (!StringUtils.isNotEmpty(colFamily)) {
      colFamily = ConstUtil.COLUMNFAMILY_DEFAULT;
    }
    TableName tablename = TableName.valueOf(tableName);
    Table table = conn.getTable(tablename);
    Put put = new Put(Bytes.toBytes(rowKey));
    for (String key : colValue.keySet()) {
      put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(key), timestamp,
          Bytes.toBytes(colValue.get(key).toString()));
    }
    table.put(put);
  }

  /**
   * 给指定的表批量添加数据
   *
   * @param tableName 表名
   * @param rows 批量插入数据实体
   * @param colFamily 列族
   */
  /*public static void batchAddRow(String tableName, String colFamily, List<RowEntity> rows)
      throws IOException {
    if (StringUtils.isEmpty(colFamily)) {
      colFamily = ConstUtil.COLUMNFAMILY_DEFAULT;
    }
    if (!CollectionUtils.isEmpty(rows)) {
      Table table = conn.getTable(TableName.valueOf(tableName));
      List<Put> list = new ArrayList<>();
      for (RowEntity row : rows) {
        if (MapUtils.isNotEmpty(row.getEntity())) {
          Put put = new Put(Bytes.toBytes(row.getRowKey()));
          Map<String, Object> map = row.getEntity();

          for (Entry<String, Object> e : map.entrySet()) {
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(e.getKey()),
                Bytes.toBytes(e.getValue().toString()));
          }
          list.add(put);
        }
      }
      table.put(list);
    }
  }*/
  public static void batchAdd(String tableName, String colFamily, List<RowEntity> rows)
      throws IOException {
    if (StringUtils.isEmpty(colFamily)) {
      colFamily = ConstUtil.COLUMNFAMILY_DEFAULT;
    }
    if (!CollectionUtils.isEmpty(rows)) {
      Table table = conn.getTable(TableName.valueOf(tableName));
      List<Put> puts = new ArrayList<>();
      for (RowEntity row : rows) {
        Put put = new Put(Bytes.toBytes(row.getRowKey()));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(row.getCol()),
            Bytes.toBytes(row.getValue()));
        puts.add(put);
      }
      table.put(puts);
    }
  }

  public static void batchAdd(List<RowEntity> rows)
      throws IOException {
    batchAdd(tableName, colName, rows);
  }

  /**
   * 删除数据
   *
   * @param tableName 表名
   * @param rowKey 行健
   */
  public void deleteRow(String tableName, String rowKey, long timestamp) throws IOException {
    Table table = conn.getTable(TableName.valueOf(tableName));
    Delete delete = new Delete(Bytes.toBytes(rowKey));
    delete.addFamilyVersion(Bytes.toBytes(ConstUtil.COLUMNFAMILY_DEFAULT), timestamp);
    table.delete(delete);
  }

  /**
   * 批量删除数据
   *
   * @param tableName 表名
   * @param rowKeys 行健
   */
//  public void batchDeleteRow(String tableName, Collection<String> rowKeys, long timestamp)
//      throws IOException {
//    List<Delete> deletes = new ArrayList<>();
//    Table table = conn.getTable(TableName.valueOf(tableName));
//    for (String rowKey : rowKeys) {
//      Delete delete = new Delete(Bytes.toBytes(rowKey));
//      delete.addFamilyVersion(Bytes.toBytes(ConstUtil.COLUMNFAMILY_DEFAULT), timestamp);
//      deletes.add(delete);
//    }
//    table.delete(deletes);
//  }

  /**
   * 精确到列的检索
   *
   * @param tableName 表名
   * @param rowKey 行健
   * @param columnfamily 列族
   * @param col 字段
   */
  public LabelResult getColumnRowData(String tableName, String rowKey, String columnfamily,
      String col) throws IOException {
    Table table = conn.getTable(TableName.valueOf(tableName));
    Get get = new Get(Bytes.toBytes(rowKey));
    if (StringUtils.isNotEmpty(columnfamily) && StringUtils.isNotEmpty(col)) {
      get.addFamily(Bytes.toBytes(columnfamily)); //指定列族
      get.addColumn(Bytes.toBytes(columnfamily), Bytes.toBytes(col));  //指定列
    }
    Result result = table.get(get);
    return ResultFormatter.formatResult(result, rowKey);
  }

  /**
   * 列族下数据
   *
   * @param tableName 表名
   * @param rowKey 行健
   * @param colFamily 列族
   */
  public LabelResult getRowData(String tableName, String rowKey, String colFamily)
      throws IOException {
    if (!StringUtils.isNotEmpty(colFamily)) {
      colFamily = ConstUtil.COLUMNFAMILY_DEFAULT;
    }
    Table table = conn.getTable(TableName.valueOf(tableName));
    Get get = new Get(Bytes.toBytes(rowKey));
    if (StringUtils.isNotEmpty(colFamily)) {
      get.addFamily(Bytes.toBytes(colFamily)); //指定列族
    }
    get.setMaxVersions();
    Result result = table.get(get);
    return ResultFormatter.formatResult(result, rowKey);
  }


  public static Boolean existsRowKey(String tableName, String rowKey) {
    Table table;
    Get get = new Get(Bytes.toBytes(rowKey));
    get.setCheckExistenceOnly(true);
    Result result;
    try {
      table = conn.getTable(TableName.valueOf(tableName));
      result = table.get(get);
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return result.getExists();
  }

  public static Boolean existsRowKey(String rowKey) {
    return existsRowKey(tableName, rowKey);
  }

  /**
   * 列族下数据
   *
   * @param tableName 表名
   * @param rowKeys 行健
   * @param colFamily 列族
   */
  public LabelResult getRowData(String tableName, Collection<String> rowKeys, String colFamily,
      long timestamp) throws IOException {
    if (StringUtils.isEmpty(colFamily)) {
      colFamily = ConstUtil.COLUMNFAMILY_DEFAULT;
    }
    Table table = conn.getTable(TableName.valueOf(tableName));
    List<Get> gets = new ArrayList();
    for (String rowKey : rowKeys) {
      Get get = new Get(Bytes.toBytes(rowKey));
      get.addFamily(Bytes.toBytes(colFamily));
      if (timestamp != 0) {
        get.setTimeStamp(timestamp);
      } else {
        get.setMaxVersions();
      }
      gets.add(get);
    }
    return ResultFormatter.formatResults(table.get(gets));
  }


}
