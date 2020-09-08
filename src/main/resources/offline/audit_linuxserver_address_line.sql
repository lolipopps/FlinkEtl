 ADD JAR /root/app/hive-2.3.7/lib/elasticsearch-hadoop-hive-6.8.3.jar; ADD JAR /root/app/hive-2.3.7/lib/commons-httpclient-3.0.1.jar; ADD JAR /root/app/hive-2.3.7/lib/hive-contrib-2.3.7.jar;

INSERT OVERWRITE TABLE ods.audit_linuxserver_address partition(stat_hour='${hiveconf:stat_hour}')
SELECT  event_sender 
       ,event_source_id 
       ,module_type 
       ,user_name 
       ,ip 
       ,substr(regexp_replace(event_local_time,"T"," "),1,19) AS event_local_time 
       ,mac 
       ,behaviour_type 
       ,center_key 
       ,log_type 
       ,event_id 
       ,size 
       ,system_type 
       ,dest_ip 
       ,event_level 
       ,style 
       ,substr(regexp_replace(center_time,"T"," "),1,19)      AS center_time 
       ,substr(regexp_replace(event_time,"T"," "),1,19)       AS event_time
FROM stg.audit_linuxserver_address
WHERE date_format(substr(regexp_replace(center_time,"T"," "),1,19),"yyyyMMddHH") = '${hiveconf:stat_hour}'; 

DROP TABLE IF EXISTS dw.audit_linuxserver_address_${hiveconf:stat_hour}; -- 数据加工
CREATE TABLE dw.audit_linuxserver_address_${hiveconf:stat_hour} AS
SELECT  user_name                                                                 AS userid 
       ,ip                                                                        AS login_ip -- 客户端id 
       ,dest_ip                                                                   AS facility_ip 
       ,mac                                                                       AS mac -- 服务器ip 
       ,MIN(unix_timestamp(center_time) - unix_timestamp(event_time) )/1000       AS min_druid -- 最小持续时间 
       ,MAX(unix_timestamp(center_time) - unix_timestamp(event_time) )/1000       AS max_druid -- 最大持续时间 
       ,FLOOR(AVG(unix_timestamp(center_time) - unix_timestamp(event_time)))/1000 AS avg_druid -- 平均持续时间 
       ,SUM(unix_timestamp(center_time) - unix_timestamp(event_time) )/1000       AS sum_druid -- 总大持续时间 
       ,COUNT(distinct style)                                                     AS style_uv -- 类型种类 
       ,COUNT(distinct event_level)                                               AS event_level_uv -- 行为种类 
       ,AVG(size)                                                                 AS avg_size
FROM ods.audit_linuxserver_address
WHERE stat_hour = '${hiveconf:stat_hour}' 
GROUP BY  user_name 
         ,ip 
         ,mac 
         ,dest_ip ; 