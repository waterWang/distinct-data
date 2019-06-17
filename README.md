


cp D:/L120190606_123.txt D:/test/1ssdddddddd.txt

disable 'ns:distinct_msg'
drop 'ns:distinct_msg'

create_namespace 'ns'
create 'ns:distinct_msg',{NAME=>'file_no',TTL=>5184000}

truncate 'ns:distinct_msg'


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

spark-submit --class com.dzyun.matches.hbase.HBaseClient --master yarn /home/tiger/distinct-data/target/distinct-data-1.0-SNAPSHOT-jar-with-dependencies.jar


spark-submit --class com.dzyun.matches.hive.HiveClient --master yarn /home/tiger/distinct-data/target/distinct-data-1.0-SNAPSHOT-jar-with-dependencies.jar

mvn clean assembly:assembly -DskipTests


set hive.cli.print.header=true;
set  hive.resultset.use.unique.column.names=false;
