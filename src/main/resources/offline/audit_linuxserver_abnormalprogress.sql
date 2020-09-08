CREATE EXTERNAL TABLE default.audit_linuxserver_abnormalprogress_2020_06_28 ( event_sender string ,event_source_id string ,per_use bigint ,module_type string ,user_name string ,ip string ,event_local_time string ,c_reserve string ,type bigint ,mac string ,behaviour_type bigint ,progress_name string ,center_key string ,log_type string ,total bigint ,event_id string ,size bigint ,system_type string ,event_level bigint ,free bigint ,per_time bigint ,center_time string ,event_time timestamp ,limit_use bigint ) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES( 'es.resource'='audit-linuxserver-abnormalprogress-2020-06-28*/doc', 'es.nodes'='kafka:9810' );

CREATE TABLE audit_linuxserver_abnormalprogres AS
SELECT  *
FROM default.audit_linuxserver_abnormalprogress_2020_06_28; -- 不正常进程情况

SELECT  user_name                                                                                                                                     AS userid 
       ,ip                                                                                                                                            AS login_ip -- 客户端id 
       ,mac                                                                                                                                           AS mac -- 服务器ip 
       ,MIN(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS min_druid -- 最小持续时间 
       ,MAX(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS max_druid -- 最大持续时间 
       ,FLOOR(AVG(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19))))/1000 AS avg_druid -- 平均持续时间 
       ,SUM(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS sum_druid -- 总大持续时间 
       ,COUNT(distinct progress_name)                                                                                                                 AS progress_name_uv -- 进程种类 
       ,COUNT(distinct type)                                                                                                                          AS type_uv -- 类型种类 
       ,COUNT(distinct behaviour_type)                                                                                                                AS behaviour_type_uv -- 行为种类 
       ,AVG(per_time)                                                                                                                                 AS avg_per_time -- 日志类型 
       ,AVG(per_use)                                                                                                                                  AS avg_per_use 
       ,AVG(size)                                                                                                                                     AS size 
       ,AVG(limit_use)                                                                                                                                AS limit_use 
       ,AVG(free)                                                                                                                                     AS free
FROM default.audit_linuxserver_abnormalprogres
GROUP BY  user_name 
         ,ip 
         ,mac 