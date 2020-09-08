CREATE EXTERNAL TABLE default.audit_linuxserver_file_2020_06_30 ( event_sender string ,event_source_id string ,operation_type bigint ,module_type string ,user_name string ,ip string ,event_local_time string ,mac string ,behaviour_type bigint ,center_key string ,log_type string ,event_id string ,size bigint ,system_type string ,file_type bigint ,event_level bigint ,center_time string ,event_time timestamp ,file_or_dir_name string ) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES( 'es.resource'='audit-linuxserver-file-2020-06-30*/doc', 'es.nodes'='kafka:9810' );


CREATE TABLE audit_linuxserver_file AS
SELECT  *
FROM audit_linuxserver_file_2020_06_30;

SELECT  user_name                                                                                                                                     AS userid 
       ,ip                                                                                                                                            AS facility_ip 
       ,mac                                                                                                                                           AS mac -- 服务器ip 
       ,CONCAT(substr(regexp_replace(center_time,"T"," "),1,15) ,"0")                                                                                 AS date_munit 
       ,MIN(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS min_druid -- 最小持续时间 
       ,MAX(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS max_druid -- 最大持续时间 
       ,FLOOR(AVG(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19))))/1000 AS avg_druid -- 平均持续时间 
       ,SUM(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS sum_druid -- 总大持续时间 
       ,COUNT(distinct file_type)                                                                                                                     AS file_type_uv -- 类型种类 
       ,COUNT(distinct file_or_dir_name)                                                                                                              AS file_or_dir_name_uv -- 行为种类 
       ,COUNT(distinct behaviour_type)                                                                                                                AS behaviour_type_uv -- 行为种类 
       ,COUNT(distinct operation_type)                                                                                                                AS operation_type_uv -- 行为种类 
       ,AVG(size)                                                                                                                                     AS avg_size
FROM default.audit_linuxserver_file
GROUP BY  user_name 
         ,ip 
         ,mac 
         ,CONCAT(substr(regexp_replace(center_time,"T"," "),1,15) ,"0");