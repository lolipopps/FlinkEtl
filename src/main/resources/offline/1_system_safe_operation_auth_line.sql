 ADD JAR /root/app/hive-2.3.7/lib/elasticsearch-hadoop-hive-6.8.3.jar;
 ADD JAR /root/app/hive-2.3.7/lib/commons-httpclient-3.0.1.jar;
 ADD JAR /root/app/hive-2.3.7/lib/hive-contrib-2.3.7.jar;

INSERT OVERWRITE TABLE ods.system_safe_operation_auth partition(stat_hour='${hiveconf:stat_hour}')
SELECT  manufacturers_facility 
       ,module_type 
       ,user_name 
       ,log_level 
       ,event_title 
       ,login_result 
       ,priority 
       ,facility_ip 
       ,login_ip 
       ,manufacturers_name 
       ,auth_method 
       ,log_type 
       ,event_type 
       ,asset_name 
       ,log_des 
       ,system_type 
       ,user_id 
       ,facility_type 
       ,facility 
       ,substr(regexp_replace(center_time,"T"," "),1,19) AS center_time 
       ,substr(regexp_replace(event_time,"T"," "),1,19)  AS event_time
FROM stg.system_safe_operation_auth
WHERE date_format(substr(regexp_replace(center_time,"T"," "),1,19),"yyyyMMddHH") = '${hiveconf:stat_hour}'; 

DROP TABLE IF EXISTS dw.system_safe_operation_auth_${hiveconf:stat_hour};
CREATE TABLE dw.system_safe_operation_auth_${hiveconf:stat_hour} AS
SELECT  user_id 
       ,login_ip 
       ,facility_ip 
       ,COUNT(distinct log_level)                                                                                                       AS log_levels 
       ,SUM(log_level)                                                                                                                  AS log_all_level 
       ,COUNT(distinct auth_method)                                                                                                     AS auth_methods 
       ,COUNT(distinct event_title)                                                                                                     AS event_titles 
       ,COUNT(distinct event_type)                                                                                                      AS event_types 
       ,COUNT(user_id)                                                                                                                  AS pv 
       ,FLOOR(AVG(priority))                                                                                                            AS priority 
       ,MIN(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(cast(event_time        AS string)) /1000) AS minDruid 
       ,MAX(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(cast(event_time       AS string)) /1000 ) AS maxDruid 
       ,FLOOR(AVG(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(cast(event_time AS string))) /1000) AS avgDruid
FROM ods.system_safe_operation_auth
WHERE stat_hour = '${hiveconf:stat_hour}' 
GROUP BY  user_id 
         ,login_ip 
         ,facility_ip; 