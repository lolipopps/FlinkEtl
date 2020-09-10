CREATE VIEW operation AS
SELECT  rowTime 
       ,getJsonObject(content,'manufacturers_facilitymodule_type') AS manufacturers_facilitymodule_type 
       ,getJsonObject(content,'user_name')                         AS user_name 
       ,getJsonObject(content,'log_level')                         AS log_level 
       ,getJsonObject(content,'event_title')                       AS event_title 
       ,getJsonObject(content,'login_result')                      AS login_result 
       ,getJsonObject(content,'syslog')                            AS syslog 
       ,getJsonObject(content,'priority')                          AS priority 
       ,getJsonObject(content,'facility_ip')                       AS facility_ip 
       ,getJsonObject(content,'login_ip')                          AS login_ip 
       ,getJsonObject(content,'manufacturers_name')                AS manufacturers_name 
       ,getJsonObject(content,'auth_method')                       AS auth_method 
       ,getJsonObject(content,'log_type')                          AS log_type 
       ,getJsonObject(content,'event_type')                        AS event_type 
       ,getJsonObject(content,'asset_name')                        AS asset_name 
       ,getJsonObject(content,'log_des')                           AS log_des 
       ,getJsonObject(content,'system_type')                       AS system_type 
       ,getJsonObject(content,'user_id')                           AS user_id 
       ,getJsonObject(content,'facility_type')                     AS facility_type 
       ,getJsonObject(content,'facility')                          AS facility 
       ,getJsonObject(content,'center_time')                       AS center_time 
       ,getJsonObject(content,'event_time')                        AS event_time
FROM all_table
WHERE logType='operation';

CREATE VIEW operation_cnt AS
SELECT  user_id
       ,login_ip
       ,facility_ip
       ,COUNT(distinct log_level)                                 AS log_levels
       ,SUM(CAST(log_level                               AS INT)) AS log_all_level
       ,COUNT(distinct auth_method)                               AS auth_methods
       ,COUNT(distinct event_title)                               AS event_titles
       ,COUNT(distinct event_type)                                AS event_types
       ,COUNT(user_id)                                            AS pv
       ,FLOOR(AVG(CAST(priority                         AS INT))) AS priority
       ,MIN(getDateDiffSecond(`event_time`,`center_time`))        AS minDruid 
       ,MAX(getDateDiffSecond(`event_time`,`center_time`))AS maxDruid 
       ,FLOOR(AVG(getDateDiffSecond(`event_time`,`center_time`))) AS avgDruid
FROM operation
GROUP BY  TUMBLE(rowTime,INTERVAL '10' MINUTE ) -- 10分钟汇总 
         ,user_id 
         ,login_ip 
         ,facility_ip;