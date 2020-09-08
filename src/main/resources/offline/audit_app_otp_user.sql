CREATE EXTERNAL TABLE default.audit_app_otp_user_2020_07_07 ( logcontent string ,userid string ,domainid string ,hostname string ,log_type string ,event_type string ,clientip string ,moduletype string ,actionid string ,serverip string ,center_time string ,actionresult string ,module_type string ,log_level bigint ,vendorid string ,priority bigint ,domianname string ,facility_ip string ,token string ,app_name string ,manufacturers_name string ,system_type string ,orgunitid string ,facility bigint ,logtime string ,event_time timestamp ) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES( 'es.resource'='audit-app-otp_user-2020-07-07*/doc', 'es.nodes'='kafka:9810' ); -- 登入行为

SELECT  userid                                                                                                                                        AS userid 
       ,clientip                                                                                                                                      AS login_ip -- 客户端id 
       ,facility_ip                                                                                                                                   AS facility_ip -- 服务器ip 
       ,CONCAT(substr(regexp_replace(center_time,"T"," "),1,15) ,"0")                                                                                 AS date_munit 
       ,MIN(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS min_druid -- 最小持续时间 
       ,MAX(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS min_druid -- 最大持续时间 
       ,SUM(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS min_druid -- 总大持续时间 
       ,FLOOR(AVG(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19))))/1000 AS avg_druid -- 平均持续时间 
       ,COUNT(distinct log_type)                                                                                                                      AS log_type_uv -- 日志类型 
       ,COUNT(distinct module_type)                                                                                                                   AS module_type_uv -- 日志类型 
       ,COUNT(distinct domainid)                                                                                                                      AS domainid_uv -- 日志类型 
       ,COUNT(distinct event_type)                                                                                                                    AS event_type_uv -- 日志类型 
       ,COUNT(distinct app_name)                                                                                                                      AS app_name_uv -- 日志类型 
       ,COUNT(distinct actionresult)                                                                                                                  AS actionresult_uv -- 日志类型 
       ,AVG(priority)                                                                                                                                 AS avg_priority -- 日志类型
FROM default.audit_app_otp_user
GROUP BY  userid 
         ,clientip 
         ,facility_ip