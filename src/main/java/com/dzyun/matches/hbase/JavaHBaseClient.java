package com.dzyun.matches.hbase;


import com.dzyun.matches.util.ConstUtil;
import com.dzyun.matches.util.LabelEntity;
import com.dzyun.matches.util.LabelResult;
import com.dzyun.matches.util.MessageException;
import com.dzyun.matches.util.ResultFormatter;
import org.apache.commons.collections.MapUtils;
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

/**
 * Hbase 通用接口
 */
public class JavaHBaseClient {

  public static Configuration conf;
  public static Connection conn;
  public static Admin admin;

  private final static String ROWKEY = "rowKey";

  public static void main(String[] args) {
    JavaHBaseClient hBaseClient = new JavaHBaseClient("dz-prod-dc1-hadoop3,dz-prod-dc1-hadoop2,dz-prod-dc1-hadoop1", "2181");
    String tableName = "ns:distinct_msg";
    try {
//      hBaseClient.insert(tableName, "qwertyupoiuyt", "file_no", "file_no", "L120190606_123");
//      LabelResult res = hBaseClient.getRowData(tableName, "qwertyupoiuyt1", "file_no");
//      System.out.println(res.getEntityList().get(0).toString());
      System.out.println(hBaseClient.existsRowKey(tableName,"c63198986baad0381569e3be4fbcfb4e12b9a050"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public JavaHBaseClient(String ip, String port) {
    conf = HBaseConfiguration.create();
    conf.set(ConstUtil.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, port);
    conf.set(ConstUtil.HBASE_ZOOKEEPER_QUORUM, ip);

    try {
      conn = ConnectionFactory.createConnection(conf);
      admin = conn.getAdmin();
    } catch (IOException e) {
      throw new MessageException("Hbase连接初始化错误", e);
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
      throw new MessageException("关闭失败", e);
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
        throw new MessageException("该表已存在");
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
      throw new MessageException("建表失败", e);
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
        throw new MessageException("该表已存在");
      } else {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
        hColumnDescriptor.setMaxVersions(ConstUtil.MAX_VERSION);
        hTableDescriptor.addFamily(hColumnDescriptor);
        admin.createTable(hTableDescriptor);
      }
    } catch (IOException e) {
      throw new MessageException("建表失败", e);
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
      throw new MessageException("删除表失败", e);
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
      throw new MessageException("关闭表失败", e);
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
      throw new MessageException("表名修改失败", e);
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
      throw new MessageException("获取表名列表失败", e);
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
  public void insert(String tableName, String rowKey, String colFamily, String col,
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
   * @param entityList 批量插入数据实体
   * @param colFamily 列族
   */
  public void bulkInsertRow(String tableName, String colFamily, List<LabelEntity> entityList,
      long version) throws IOException {
    if (!StringUtils.isNotEmpty(colFamily)) {
      colFamily = ConstUtil.COLUMNFAMILY_DEFAULT;
    }
    if (!CollectionUtils.isEmpty(entityList)) {
      TableName tablename = TableName.valueOf(tableName);
      Table table = conn.getTable(tablename);
      List<Put> list = new ArrayList<>();
      for (LabelEntity entity : entityList) {
        if (MapUtils.isNotEmpty(entity.getEntity())) {
          Put put = new Put(Bytes.toBytes(entity.getRowKey()));
          Map<String, Object> map = entity.getEntity();
          for (String key : map.keySet()) {
            put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(key), version,
                Bytes.toBytes(map.get(key).toString()));
          }
          list.add(put);
        }
      }
      table.put(list);
    }
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


  public Boolean existsRowKey(String tableName, String rowKey) {
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
