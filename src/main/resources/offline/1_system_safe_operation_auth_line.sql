 -- 用户登入日志



SELECT  user_id 
       ,login_ip 
       ,facility_ip 
       ,COUNT(distinct log_level)                                                                                                 AS log_levels 
       ,SUM(log_level)                                                                                                            AS log_all_level 
       ,COUNT(distinct auth_method)                                                                                               AS auth_methods 
       ,COUNT(distinct event_title)                                                                                               AS event_titles 
       ,COUNT(distinct event_type)                                                                                                AS event_types 
       ,COUNT(user_id)                                                                                                            AS pv 
       ,FLOOR(AVG(priority))                                                                                                      AS priority 
       ,MIN(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(cast(event_time       AS string)) ) AS minDruid 
       ,MAX(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(cast(event_time       AS string)) ) AS minDruid 
       ,FLOOR(AVG(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(cast(event_time AS string)))) AS avgDruid
FROM default.system_safe_operation_auth
GROUP BY  user_id 
         ,login_ip 
         ,facility_ip; 
