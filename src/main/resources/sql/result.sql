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
INSERT INTO kafkaSinkTable(user_id, login_ip, mac,date_munit,all_pv,avg_druid,log_type)
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