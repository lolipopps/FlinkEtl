 ADD JAR /root/app/hive-2.3.7/lib/elasticsearch-hadoop-hive-6.8.3.jar; 
 ADD JAR /root/app/hive-2.3.7/lib/commons-httpclient-3.0.1.jar; 
 ADD JAR /root/app/hive-2.3.7/lib/hive-contrib-2.3.7.jar; -- 数据清洗

INSERT OVERWRITE TABLE ods.audit_app_otp_user partition(stat_hour='${hiveconf:stat_hour}')
SELECT  logcontent 
       ,userid 
       ,domainid 
       ,hostname 
       ,log_type 
       ,event_type 
       ,clientip 
       ,moduletype 
       ,actionid 
       ,serverip 
       ,substr(regexp_replace(center_time,"T"," "),1,19) AS center_time 
       ,actionresult 
       ,module_type 
       ,log_level 
       ,vendorid 
       ,priority 
       ,domianname 
       ,facility_ip 
       ,token 
       ,app_name 
       ,manufacturers_name 
       ,system_type 
       ,orgunitid 
       ,facility 
       ,substr(regexp_replace(logtime,"T"," "),1,19)     AS logtime ;
       ,substr(regexp_replace(event_time,"T"," "),1,19)  AS event_time
FROM stg.audit_app_otp_user
WHERE date_format(substr(regexp_replace(center_time,"T"," "),1,19),"yyyyMMddHH") = '${hiveconf:stat_hour}'; 

DROP TABLE IF EXISTS dw.audit_app_otp_user_${hiveconf:stat_hour}; -- 数据加工
CREATE TABLE dw.audit_app_otp_user_${hiveconf:stat_hour} AS
SELECT  userid                                                                    AS userid 
       ,clientip                                                                  AS login_ip -- 客户端id 
       ,facility_ip                                                               AS facility_ip -- 服务器ip 
       ,MIN(unix_timestamp(center_time) - unix_timestamp(event_time) )/1000       AS min_druid -- 最小持续时间 
       ,MAX(unix_timestamp(center_time) - unix_timestamp(event_time) )/1000       AS max_druid -- 最大持续时间 
       ,SUM(unix_timestamp(center_time) - unix_timestamp(event_time) )/1000       AS sum_druid -- 总大持续时间 
       ,FLOOR(AVG(unix_timestamp(center_time) - unix_timestamp(event_time)))/1000 AS avg_druid -- 平均持续时间 
       ,COUNT(distinct log_type)                                                  AS log_type_uv -- 日志类型 
       ,COUNT(distinct module_type)                                               AS module_type_uv -- 日志类型 
       ,COUNT(distinct domainid)                                                  AS domainid_uv -- 日志类型 
       ,COUNT(distinct event_type)                                                AS event_type_uv -- 日志类型 
       ,COUNT(distinct app_name)                                                  AS app_name_uv -- 日志类型 
       ,COUNT(distinct actionresult)                                              AS actionresult_uv -- 日志类型 
       ,AVG(priority)                                                             AS avg_priority -- 日志类型
FROM ods.audit_app_otp_user
WHERE stat_hour = '${hiveconf:stat_hour}' 
GROUP BY  userid 
         ,clientip 
         ,facility_ip;