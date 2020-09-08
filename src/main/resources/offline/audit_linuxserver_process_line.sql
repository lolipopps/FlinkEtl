 ADD JAR /root/app/hive-2.3.7/lib/elasticsearch-hadoop-hive-6.8.3.jar; ADD JAR /root/app/hive-2.3.7/lib/commons-httpclient-3.0.1.jar; ADD JAR /root/app/hive-2.3.7/lib/hive-contrib-2.3.7.jar; -- 数据清洗

INSERT OVERWRITE TABLE ods.audit_linuxserver_process partition(stat_hour='${hiveconf:stat_hour}')
SELECT  user_name 
       ,mac 
       ,behaviour_type 
       ,log_type 
       ,substr(regexp_replace(center_time,"T"," "),1,19)      AS center_time 
       ,event_sender 
       ,event_source_id 
       ,process 
       ,module_type 
       ,ul_pid 
       ,ip 
       ,end_time 
       ,substr(regexp_replace(event_local_time,"T"," "),1,19) AS event_local_time 
       ,cp_path_name 
       ,paramerter 
       ,substr(regexp_replace(start_time,"T"," "),1,19)       AS start_time 
       ,center_key 
       ,event_id 
       ,ul_memory_value 
       ,size 
       ,system_type 
       ,event_level 
       ,ul_action 
       ,ul_style 
       ,substr(regexp_replace(center_time,"T"," "),1,19) event_time
FROM stg.audit_linuxserver_process
WHERE date_format(substr(regexp_replace(center_time,"T"," "),1,19),"yyyyMMddHH") = '${hiveconf:stat_hour}'; 

-- 数据加工
DROP TABLE IF EXISTS dw.audit_linuxserver_process_${hiveconf:stat_hour}; 
CREATE TABLE dw.audit_linuxserver_process_${hiveconf:stat_hour} AS
SELECT  user_name                                             AS userid 
       ,ip                                                    AS login_ip -- 客户端id 
       ,mac                                                   AS mac -- 服务器ip 
       ,COUNT(ip)                                             AS pv 
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
FROM ods.audit_linuxserver_process
GROUP BY  user_name 
         ,ip 
         ,mac 