


cp D:/L120190606_123.txt D:/test/1ssdddddddd.txt

=============hbase=========

disable 'ns:distinct_msg'  //禁用hbase表
drop 'ns:distinct_msg'     //删除hbase表
create_namespace 'ns'     //创建ns空间
create 'ns:distinct_msg',{NAME=>'file_no',TTL=>5184000}   //创建ns:distinct_msg表
truncate 'ns:distinct_msg'  //清空ns:distinct_msg表
scan 'ns:distinct_msg' , {LIMIT => 5}   // 显示表的前5行
count 'ns:distinct_msg'    // 统计表


=============hive=========

CREATE  TABLE IF NOT EXISTS  tmp.tmp_msg_www_0630(
  `phone_id` String  COMMENT 'phone_id',
  `create_time` BIGINT  COMMENT 'create_time',
  `app_name` String  COMMENT 'app_name',
  `main_call_no` String COMMENT 'main_call_no',
  `msg` String COMMENT 'msg'
)
PARTITIONED  by (the_date String,file_no String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS orc;


 // 设置查询结果显示字段名
set hive.cli.print.header=true;                    //  查询结果 表名.字段名
set  hive.resultset.use.unique.column.names=false;  //  查询结果 字段名

=============spark submit=========

spark-submit --class com.dzyun.matches.hbase.HBaseClient --master yarn  --queue root.file_to_dz /home/tiger/distinct-data/target/distinct-data-1.0-SNAPSHOT-jar-with-dependencies.jar

spark-submit --class com.dzyun.matches.hive.HiveClient --master yarn  --queue root.file_to_dz /home/tiger/distinct-data/target/distinct-data-1.0-SNAPSHOT-jar-with-dependencies.jar

spark-submit --class com.dzyun.matches.streaming.SparkStreaming --master yarn --queue root.file_to_dz /home/tiger/distinct-data/target/distinct-data-1.0-SNAPSHOT-jar-with-dependencies.jar


mvn clean assembly:assembly -DskipTests  //打包


set hive.cli.print.header=true;
set  hive.resultset.use.unique.column.names=false;


curl "https://dpapi.yibangcredit.com/receiveserver/api/v1/token?username=tianshang002&password=mreiuyhnlighean7482il"

row_key#!^id#!^isp_code#!^post_code#!^admin_code#!^area_code#!^event_time
