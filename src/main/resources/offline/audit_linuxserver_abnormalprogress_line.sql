ADD JAR /root/app/hive-2.3.7/lib/elasticsearch-hadoop-hive-6.8.3.jar;
ADD JAR /root/app/hive-2.3.7/lib/commons-httpclient-3.0.1.jar; 
ADD JAR /root/app/hive-2.3.7/lib/hive-contrib-2.3.7.jar;

INSERT OVERWRITE TABLE ods.audit_linuxserver_abnormalprogress partition(stat_hour='${hiveconf:stat_hour}')
SELECT  event_sender 
       ,event_source_id 
       ,per_use 
       ,module_type 
       ,user_name 
       ,ip 
       ,substr(regexp_replace(event_local_time,"T"," "),1,19) AS event_local_time 
       ,c_reserve 
       ,type 
       ,mac 
       ,behaviour_type 
       ,progress_name 
       ,center_key 
       ,log_type 
       ,total 
       ,event_id 
       ,size 
       ,system_type 
       ,event_level 
       ,free 
       ,per_time 
       ,substr(regexp_replace(center_time,"T"," "),1,19)      AS center_time 
       ,substr(regexp_replace(event_time,"T"," "),1,19)       AS event_time 
       ,limit_use
FROM stg.audit_linuxserver_abnormalprogress
WHERE date_format(substr(regexp_replace(center_time,"T"," "),1,19),"yyyyMMddHH") = '${hiveconf:stat_hour}'; 

DROP TABLE IF EXISTS dw.audit_linuxserver_abnormalprogres_${hiveconf:stat_hour}; -- 数据加工
CREATE TABLE dw.audit_linuxserver_abnormalprogres_${hiveconf:stat_hour} AS
SELECT  user_name                                                                 AS userid 
       ,ip                                                                        AS login_ip -- 客户端id 
       ,mac                                                                       AS mac -- 服务器ip 
       ,MIN(unix_timestamp(center_time) - unix_timestamp(event_time))/1000        AS min_druid -- 最小持续时间 
       ,MAX(unix_timestamp(center_time) - unix_timestamp(event_time) )/1000       AS max_druid -- 最大持续时间 
       ,FLOOR(AVG(unix_timestamp(center_time) - unix_timestamp(event_time)))/1000 AS avg_druid -- 平均持续时间 
       ,SUM(unix_timestamp(center_time) - unix_timestamp(event_time) )/1000       AS sum_druid -- 总大持续时间 
       ,COUNT(distinct progress_name)                                             AS progress_name_uv -- 进程种类 
       ,COUNT(distinct type)                                                      AS type_uv -- 类型种类 
       ,COUNT(distinct behaviour_type)                                            AS behaviour_type_uv -- 行为种类 
       ,AVG(per_time)                                                             AS avg_per_time -- 日志类型 
       ,AVG(per_use)                                                              AS avg_per_use 
       ,AVG(size)                                                                 AS size 
       ,AVG(limit_use)                                                            AS limit_use 
       ,AVG(free)                                                                 AS free
FROM ods.audit_linuxserver_abnormalprogres
WHERE stat_hour = '${hiveconf:stat_hour}' 
GROUP BY  user_name 
         ,ip 
         ,mac 