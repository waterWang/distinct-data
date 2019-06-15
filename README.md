


cp D:/L120190606_123.txt D:/test/1ssdddddddd.txt

disable 'ns:distinct_msg'
drop 'ns:distinct_msg'

create_namespace 'ns'
create 'ns:distinct_msg',{NAME=>'file_no',TTL=>5184000}

truncate 'ns:distinct_msg'


create database tmp;

CREATE  TABLE IF NOT EXISTS  tmp.tmp_msg(
  `app_name` String  COMMENT '应用名字',
  `phone_id` String COMMENT '手机号',
  `event_time` String COMMENT '短信生产时间格式化 yyyy-MM-dd HH:mm:ss',
  `msg` String COMMENT '短信内容',
  `main_call_no` String COMMENT '主叫号码'
)
PARTITIONED  by (thedate String, file_no String)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS textfile;