DROP TABLE audit_linuxserver_network_2020_06;
CREATE EXTERNAL TABLE default.audit_linuxserver_network_2020_06 
( event_sender string ,event_source_id string ,module_type string 
,local_port bigint ,user_name string ,ip string ,remote_port bigint 
,event_local_time string ,protocal_type bigint ,mac string 
,center_key string ,domain_name string ,log_type string 
,protocol string ,event_id string ,remote_ip string 
,size bigint ,system_type string ,process_name string 
,event_level bigint ,style bigint ,center_time string 
,event_time timestamp ) 
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' 
TBLPROPERTIES( 'es.resource'='audit-linuxserver-network-2020-06*/doc', 'es.nodes'='kafka:9810' );

CREATE TABLE audit_linuxserver_network AS
SELECT  *
FROM default.audit_linuxserver_network_2020_06;

SELECT  user_name                                                                                                                                     AS userid 
       ,ip                                                                                                                                            AS login_ip -- 客户端id 
       ,remote_ip                                                                                                                                     AS facility_ip 
       ,mac                                                                                                                                           AS mac -- 服务器ip 
       ,CONCAT(substr(regexp_replace(center_time,"T"," "),1,15) ,"0")                                                                                 AS date_munit 
       ,MIN(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS min_druid -- 最小持续时间 
       ,MAX(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS max_druid -- 最大持续时间 
       ,FLOOR(AVG(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19))))/1000 AS avg_druid -- 平均持续时间 
       ,SUM(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS sum_druid -- 总大持续时间 
       ,COUNT(distinct style)                                                                                                                         AS style_uv -- 类型种类 
       ,COUNT(distinct event_level)                                                                                                                   AS event_level_uv -- 行为种类 
       ,COUNT(distinct process_name)                                                                                                                  AS process_name_uv 
       ,AVG(size)                                                                                                                                     AS avg_size
FROM default.audit_linuxserver_network
GROUP BY  user_name 
         ,ip 
         ,mac 
         ,remote_ip 
         ,CONCAT(substr(regexp_replace(center_time,"T"," "),1,15) ,"0");