 ADD JAR /root/app/hive-2.3.7/lib/elasticsearch-hadoop-hive-6.8.3.jar; 
 ADD JAR /root/app/hive-2.3.7/lib/commons-httpclient-3.0.1.jar; 
 ADD JAR /root/app/hive-2.3.7/lib/hive-contrib-2.3.7.jar; -- 数据清洗

INSERT OVERWRITE TABLE ods.audit_linuxserver_file partition(stat_hour='2020062903')
SELECT  event_sender 
       ,event_source_id 
       ,operation_type 
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
       ,file_type 
       ,event_level 
       ,substr(regexp_replace(center_time,"T"," "),1,19)      AS center_time 
       ,substr(regexp_replace(event_time,"T"," "),1,19)       AS event_time 
       ,file_or_dir_name
FROM stg.audit_linuxserver_file
WHERE date_format(substr(regexp_replace(center_time,"T"," "),1,19),"yyyyMMddHH") = '2020062903'; 


DROP TABLE IF EXISTS dw.audit_linuxserver_file_2020062903; -- 数据加工
CREATE TABLE dw.audit_linuxserver_file_2020062903 AS
SELECT  user_name                                                                 AS userid 
       ,ip                                                                        AS facility_ip 
       ,mac                                                                       AS mac -- 服务器ip 
       ,MIN(unix_timestamp(center_time) - unix_timestamp(event_time) )/1000       AS min_druid -- 最小持续时间 
       ,MAX(unix_timestamp(center_time) - unix_timestamp(event_time) )/1000       AS max_druid -- 最大持续时间 
       ,FLOOR(AVG(unix_timestamp(center_time) - unix_timestamp(event_time)))/1000 AS avg_druid -- 平均持续时间 
       ,SUM(unix_timestamp(center_time) - unix_timestamp(event_time) )/1000       AS sum_druid -- 总大持续时间 
       ,COUNT(distinct file_type)                                                 AS file_type_uv -- 类型种类 
       ,COUNT(distinct file_or_dir_name)                                          AS file_or_dir_name_uv -- 行为种类 
       ,COUNT(distinct behaviour_type)                                            AS behaviour_type_uv -- 行为种类 
       ,COUNT(distinct operation_type)                                            AS operation_type_uv -- 行为种类 
       ,AVG(size)                                                                 AS avg_size
FROM ods.audit_linuxserver_file
WHERE stat_hour = '2020062903' 
GROUP BY  user_name 
         ,ip 
         ,mac ;