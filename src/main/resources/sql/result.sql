CREATE TABLE kafkaSinkTable (
user_id STRING,
login_ip STRING,
mac STRING,
date_munit STRING,
all_pv BIGINT,
avg_druid BIGINT,
log_type STRING
) WITH (
'connector' = 'kafka-0.10',
'topic' = 'flinkEtlSink',
'properties.bootstrap.servers' = 'kafka:9092',
'properties.group.id' = 'flinkEtlSink',
'format' = 'json',
'scan.startup.mode' = 'earliest-offset'
);

CREATE TABLE MysqlSinkTable (
user_id VARCHAR,
login_ip VARCHAR,
mac VARCHAR,
date_munit VARCHAR,
all_pv BIGINT,
avg_druid BIGINT,
log_type VARCHAR
) WITH (
    'connector.type' = 'jdbc',
    'connector.url' = 'jdbc:mysql://mysql:3306/flink',
    'connector.table' = 'mysql_sink_table', -- 表名
    'connector.username' = 'root', -- 用户名
    'connector.password' = 'hu1234tai', -- 密码
    'connector.write.flush.max-rows' = '5'   -- 刷新数量
);


CREATE VIEW  temp as
select
    user_id,
    login_ip,
    mac,
    date_munit,
    all_pv,
    avg_druid,
    'file' as log_type
FROM `file_cnt`
union all
select
    user_id,
    login_ip,
    mac,
    date_munit,
    all_pv,
    avg_druid,
    'process' as log_type
FROM `process_cnt`;

INSERT INTO kafkaSinkTable(user_id, login_ip, mac,date_munit,all_pv,avg_druid,log_type)
select user_id, login_ip, mac,date_munit,all_pv,avg_druid,log_type from temp;


INSERT INTO MysqlSinkTable(user_id, login_ip, mac,date_munit,all_pv,avg_druid,log_type)
select user_id, login_ip, mac,date_munit,all_pv,avg_druid,log_type from temp;
