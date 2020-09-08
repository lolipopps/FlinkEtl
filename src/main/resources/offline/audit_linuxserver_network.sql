 ADD JAR /root/app/hive-2.3.7/lib/elasticsearch-hadoop-hive-6.8.3.jar; ADD JAR /root/app/hive-2.3.7/lib/commons-httpclient-3.0.1.jar; ADD JAR /root/app/hive-2.3.7/lib/hive-contrib-2.3.7.jar; -- 数据清洗

INSERT OVERWRITE TABLE ods.audit_linuxserver_network partition(stat_hour='2020053116')
SELECT  event_sender 
       ,event_source_id 
       ,module_type 
       ,local_port 
       ,user_name 
       ,ip 
       ,remote_port 
       ,substr(regexp_replace(event_local_time,"T"," "),1,19) AS event_local_time 
       ,protocal_type 
       ,mac 
       ,center_key 
       ,domain_name 
       ,log_type 
       ,protocol 
       ,event_id 
       ,remote_ip 
       ,size 
       ,system_type 
       ,process_name 
       ,event_level 
       ,style 
       ,event_time 
       ,substr(regexp_replace(event_local_time,"T"," "),1,19) AS center_time
FROM stg.audit_linuxserver_network
WHERE date_format(substr(regexp_replace(center_time,"T"," "),1,19),"yyyyMMddHH") = '2020053116'; 

DROP TABLE IF EXISTS dw.audit_linuxserver_network_2020053116; -- 数据加工
CREATE TABLE dw.audit_linuxserver_network_2020053116 AS
SELECT  user_name                                                                 AS userid 
       ,ip                                                                        AS login_ip -- 客户端id 
       ,remote_ip                                                                 AS facility_ip 
       ,mac                                                                       AS mac -- 服务器ip 
       ,MIN(unix_timestamp(center_time) - unix_timestamp(event_time) )/1000       AS min_druid -- 最小持续时间 
       ,MAX(unix_timestamp(center_time) - unix_timestamp(event_time) )/1000       AS max_druid -- 最大持续时间 
       ,FLOOR(AVG(unix_timestamp(center_time) - unix_timestamp(event_time)))/1000 AS avg_druid -- 平均持续时间 
       ,SUM(unix_timestamp(center_time) - unix_timestamp(event_time) )/1000       AS sum_druid -- 总大持续时间 
       ,COUNT(distinct style)                                                     AS style_uv -- 类型种类 
       ,COUNT(distinct event_level)                                               AS event_level_uv -- 行为种类 
       ,COUNT(distinct process_name)                                              AS process_name_uv 
       ,AVG(size)                                                                 AS avg_size
FROM ods.audit_linuxserver_network
GROUP BY  user_name 
         ,ip 
         ,mac 
         ,remote_ip ;