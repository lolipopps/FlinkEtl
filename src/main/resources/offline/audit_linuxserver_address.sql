CREATE EXTERNAL TABLE default.audit_linuxserver_address_2020_06_24 
( event_sender string ,event_source_id string ,module_type string 
,user_name string ,ip string ,event_local_time string ,mac string ,behaviour_type bigint ,center_key string ,log_type string ,event_id string ,size bigint ,system_type string ,dest_ip string ,event_level bigint ,style bigint ,center_time string ,event_time timestamp ) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES( 'es.resource'='audit-linuxserver-address-2020-06-24*/doc', 'es.nodes'='kafka:9810' );

CREATE TABLE audit_linuxserver_address AS
SELECT  *
FROM default.audit_linuxserver_address_2020_06_24;

SELECT  user_name                                                                                                                                     AS userid 
       ,ip                                                                                                                                            AS login_ip -- 客户端id 
       ,dest_ip                                                                                                                                       AS facility_ip 
       ,mac                                                                                                                                           AS mac -- 服务器ip 
       ,CONCAT(substr(regexp_replace(center_time,"T"," "),1,15) ,"0")                                                                                 AS date_munit 
       ,MIN(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS min_druid -- 最小持续时间 
       ,MAX(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS max_druid -- 最大持续时间 
       ,FLOOR(AVG(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19))))/1000 AS avg_druid -- 平均持续时间 
       ,SUM(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS sum_druid -- 总大持续时间 
       ,COUNT(distinct style)                                                                                                                         AS style_uv -- 类型种类 
       ,COUNT(distinct event_level)                                                                                                                   AS event_level_uv -- 行为种类 
       ,AVG(size)                                                                                                                                     AS avg_size
FROM default.audit_linuxserver_address
GROUP BY  user_name 
         ,ip 
         ,mac 
         ,dest_ip 
         ,CONCAT(substr(regexp_replace(center_time,"T"," "),1,15) ,"0");