CREATE EXTERNAL TABLE default.audit_linuxserver_process_2020_06_30 ( user_name string ,mac string ,behaviour_type bigint ,log_type string ,center_time string ,event_sender string ,event_source_id string ,process string ,module_type string ,ul_pid bigint ,ip string ,end_time string ,event_local_time string ,cp_path_name string ,paramerter string ,start_time string ,center_key string ,event_id string ,ul_memory_value string ,size bigint ,system_type string ,event_level bigint ,ul_action bigint ,ul_style bigint ,event_time timestamp ) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES( 'es.resource'='audit-linuxserver-process-2020-06-30*/doc', 'es.nodes'='kafka:9810' );

CREATE TABLE audit_linuxserver_process AS
SELECT  *
FROM default.audit_linuxserver_process_2020_06_30;

SELECT  user_name                                                                                                                                     AS userid 
       ,ip                                                                                                                                            AS login_ip -- 客户端id 
       ,mac                                                                                                                                           AS mac -- 服务器ip 
       ,COUNT(ip)                                                                                                                                     AS pv 
       ,CONCAT(substr(regexp_replace(center_time,'T',' '),1,15) ,'0')                                                                                 AS date_munit 
       ,MIN(unix_timestamp(substr(regexp_replace(center_time,'T',' '),1,19)) - unix_timestamp(substr(regexp_replace(event_time,'T',' '),1,19)) )/1000 AS min_druid -- 最小持续时间 
       ,MAX(unix_timestamp(substr(regexp_replace(center_time,'T',' '),1,19)) - unix_timestamp(substr(regexp_replace(event_time,'T',' '),1,19)) )/1000 AS max_druid -- 最大持续时间 
       ,FLOOR(AVG(unix_timestamp(substr(regexp_replace(center_time,'T',' '),1,19)) - unix_timestamp(substr(regexp_replace(event_time,'T',' '),1,19))))/1000 AS avg_druid -- 平均持续时间 
       ,SUM(unix_timestamp(substr(regexp_replace(center_time,'T',' '),1,19)) - unix_timestamp(substr(regexp_replace(event_time,'T',' '),1,19)) )/1000 AS sum_druid -- 总大持续时间 
       ,COUNT(distinct process)                                                                                                                       AS process_uv -- 进程种类 
       ,COUNT(distinct ul_action)                                                                                                                     AS ul_action_uv -- 类型种类 
       ,COUNT(distinct ul_style)                                                                                                                      AS ul_style_uv -- 类型种类 
       ,COUNT(distinct ul_pid)                                                                                                                        AS ul_pid_uv -- 类型种类 
       ,COUNT(distinct behaviour_type)                                                                                                                AS behaviour_type_uv -- 行为种类 
       ,COUNT(distinct event_level)                                                                                                                   AS event_level_uv -- 行为种类 
       ,AVG(size)                                                                                                                                     AS size
FROM default.audit_linuxserver_process
GROUP BY  user_name 
         ,ip 
         ,mac 
         ,CONCAT(substr(regexp_replace(center_time,'T',' '),1,15) ,'0') ;

SELECT  user_name                                             AS userid 
       ,ip                                                    AS login_ip -- 客户端id 
       ,mac                                                   AS mac -- 服务器ip 
       ,COUNT(ip)                                             AS pv 
       ,getDateFormat(center_time)                            AS date_munit 
       ,MIN(getDateDiffSecond(event_time,center_time))        AS min_druid -- 最小持续时间 
       ,MAX(getDateDiffSecond(event_time,center_time))        AS max_druid -- 最大持续时间 
       ,FLOOR(AVG(getDateDiffSecond(event_time,center_time))) AS avg_druid -- 平均持续时间 
       ,SUM( AVG(getDateDiffSecond(event_time,center_time)))  AS sum_druid -- 总大持续时间 
       ,COUNT(distinct process)                               AS process_uv -- 进程种类 
       ,COUNT(distinct ul_action)                             AS ul_action_uv -- 类型种类 
       ,COUNT(distinct ul_style)                              AS ul_style_uv -- 类型种类 
       ,COUNT(distinct ul_pid)                                AS ul_pid_uv -- 类型种类 
       ,COUNT(distinct behaviour_type)                        AS behaviour_type_uv -- 行为种类 
       ,COUNT(distinct event_level)                           AS event_level_uv -- 行为种类 
       ,AVG(cast(size                                AS int)) AS size
FROM default.audit_linuxserver_process
GROUP BY  user_name 
         ,ip 
         ,mac 
         ,getDateFormat(center_time) 