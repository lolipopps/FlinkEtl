CREATE EXTERNAL TABLE default.system_safe_attack_ips_2020_06_17 ( event_son_type string ,attack_type string ,manufacturers_facility string ,facility_hostname string ,threat_id string ,hit_direction string ,protection string ,zone_name string ,syslog string ,severity_level string ,sub_protection string ,log_type string ,cve string ,event_type string ,attack_sub_category string ,app_protocol string ,dest_zone_name string ,facility_type string ,conduct_operations string ,center_time string ,dest_port float ,module_type string ,ip string ,log_level float ,policy_name string ,abstract string ,priority float ,facility_ip string ,manufacturers_name string ,protocol_type string ,threat_name string ,log_des string ,port float ,system_type string ,vpn string ,dest_ip string ,bid string ,facility float ,msb string ,event_time timestamp ) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES( 'es.resource'='system-safe-attack_ips-2020-06-17*/doc', 'es.nodes'='kafka:9810' );

SELECT  *
FROM system_safe_attack_ips_2020_06_17
LIMIT 10;

CREATE TABLE system_safe_attack_ips AS
SELECT  *
FROM system_safe_attack_ips_2020_06_17;

SELECT  module_type -- 安全类型 
       ,COUNT(*)
FROM system_safe_attack_ips
GROUP BY  module_type;

SELECT  log_type -- 安全类型 
       ,COUNT(*)
FROM system_safe_attack_ips
GROUP BY  log_type;

SELECT  attack_type -- 安全类型 
       ,COUNT(*)
FROM system_safe_attack_ips
GROUP BY  attack_type;

SELECT  attack_sub_category -- 安全类型 
       ,COUNT(*)
FROM system_safe_attack_ips
GROUP BY  attack_sub_category;

SELECT  severity_level -- 安全类型 
       ,COUNT(*)
FROM system_safe_attack_ips
GROUP BY  severity_level;

SELECT  ip                                                                                                                                            AS login_ip -- 登入ip 
       ,facility_ip                                                                                                                                   AS facility_ip -- 访问id 
       ,CONCAT(substr(regexp_replace(center_time,"T"," "),1,15) ,"0")                                                                                 AS start_time 
       ,COUNT(ip)                                                                                                                                     AS pv -- 登入次数 
       ,AVG(case WHEN attack_sub_category is not null AND attack_sub_category != '' THEN 1 else 0 end )                                               AS attack_sub_pv -- 子攻击种类 
       ,COUNT(distinct attack_sub_category)                                                                                                           AS attack_sub_uv -- 子攻击次数 
       ,AVG(case WHEN attack_type is not null AND attack_type != '' THEN 1 else 0 end)                                                                AS attack_pv -- 攻击次数 
       ,COUNT(distinct attack_type)                                                                                                                   AS attack_uv -- 攻击种类 
       ,COUNT(distinct bid) + COUNT(distinct cve) + COUNT(distinct msb)                                                                               AS real_attack -- 已知攻击种类 
       ,COUNT(distinct log_level)                                                                                                                     AS log_levels -- 日志种类 
       ,AVG(log_level)                                                                                                                                AS log_all_level -- 日志所有级别 
       ,COUNT(distinct protection)                                                                                                                    AS protection_uv 
       ,COUNT(distinct protocol_type)                                                                                                                 AS protocol_uv 
       ,COUNT(distinct port)                                                                                                                          AS port_uv 
       ,COUNT(distinct threat_id)                                                                                                                     AS threat_uv 
       ,FLOOR(AVG(priority))                                                                                                                          AS priority 
       ,SUM(case WHEN severity_level = 'INVALID' THEN 1 else 0 end)                                                                                   AS invalid_cnt 
       ,SUM(case WHEN severity_level = '低' THEN 1 else 0 end)                                                                                         AS low_cnt 
       ,SUM(case WHEN severity_level = '中' THEN 1 else 0 end)                                                                                         AS medium_cnt 
       ,SUM(case WHEN severity_level = '高' THEN 1 else 0 end)                                                                                         AS high_cnt 
       ,SUM(case WHEN severity_level = '严重' THEN 1 else 0 end)                                                                                        AS critical_cnt 
       ,MIN(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS min_druid -- 最小持续时间 
       ,MAX(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS min_druid -- 最大持续时间 
       ,SUM(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19)) )/1000 AS min_druid -- 总大持续时间 
       ,FLOOR(AVG(unix_timestamp(substr(regexp_replace(center_time,"T"," "),1,19)) - unix_timestamp(substr(regexp_replace(event_time,"T"," "),1,19))))/1000 AS avg_druid -- 平均持续时间 
       ,COUNT(distinct module_type)                                                                                                                   AS module_type_uv 
       ,SUM(case WHEN module_type = 'safe' THEN 1 else null end)                                                                                      AS safe_cnt
FROM default.system_safe_attack_ips
GROUP BY  ip 
         ,facility_ip 
         ,CONCAT(substr(regexp_replace(center_time,"T"," "),1,15) ,"0")