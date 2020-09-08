 ADD JAR /root/app/hive-2.3.7/lib/elasticsearch-hadoop-hive-6.8.3.jar; ADD JAR /root/app/hive-2.3.7/lib/commons-httpclient-3.0.1.jar; ADD JAR /root/app/hive-2.3.7/lib/hive-contrib-2.3.7.jar;

INSERT OVERWRITE TABLE ods.system_safe_attack_ips partition(stat_hour='2020061618')
SELECT  event_son_type 
       ,attack_type 
       ,manufacturers_facility 
       ,facility_hostname 
       ,threat_id 
       ,hit_direction 
       ,protection 
       ,zone_name 
       ,severity_level 
       ,sub_protection 
       ,log_type 
       ,cve 
       ,event_type 
       ,attack_sub_category 
       ,app_protocol 
       ,dest_zone_name 
       ,facility_type 
       ,conduct_operations 
       ,substr(regexp_replace(center_time,"T"," "),1,19) center_time 
       ,dest_port 
       ,module_type 
       ,ip 
       ,log_level 
       ,policy_name 
       ,abstract 
       ,priority 
       ,facility_ip 
       ,manufacturers_name 
       ,protocol_type 
       ,threat_name 
       ,log_des 
       ,port 
       ,system_type 
       ,vpn 
       ,dest_ip 
       ,bid 
       ,facility 
       ,msb 
       ,substr(regexp_replace(event_time,"T"," "),1,19) AS event_time
FROM stg.system_safe_attack_ips
WHERE date_format(substr(regexp_replace(center_time,"T"," "),1,19),"yyyyMMddHH") = '2020061618'; 

DROP TABLE IF EXISTS dw.system_safe_attack_ips_2020061618;
CREATE TABLE dw.system_safe_attack_ips_2020061618 AS
SELECT  ip                                                                                              AS login_ip -- 登入ip 
       ,facility_ip                                                                                     AS facility_ip -- 访问id 
       ,CONCAT(substr(regexp_replace(center_time,"T"," "),1,15) ,"0")                                   AS start_time 
       ,COUNT(ip)                                                                                       AS pv -- 登入次数 
       ,AVG(case WHEN attack_sub_category is not null AND attack_sub_category != '' THEN 1 else 0 end ) AS attack_sub_pv -- 子攻击种类 
       ,COUNT(distinct attack_sub_category)                                                             AS attack_sub_uv -- 子攻击次数 
       ,AVG(case WHEN attack_type is not null AND attack_type != '' THEN 1 else 0 end)                  AS attack_pv -- 攻击次数 
       ,COUNT(distinct attack_type)                                                                     AS attack_uv -- 攻击种类 
       ,COUNT(distinct bid) + COUNT(distinct cve) + COUNT(distinct msb)                                 AS real_attack -- 已知攻击种类 
       ,COUNT(distinct log_level)                                                                       AS log_levels -- 日志种类 
       ,AVG(log_level)                                                                                  AS log_all_level -- 日志所有级别 
       ,COUNT(distinct protection)                                                                      AS protection_uv 
       ,COUNT(distinct protocol_type)                                                                   AS protocol_uv 
       ,COUNT(distinct port)                                                                            AS port_uv 
       ,COUNT(distinct threat_id)                                                                       AS threat_uv 
       ,FLOOR(AVG(priority))                                                                            AS priority 
       ,SUM(case WHEN severity_level = 'INVALID' THEN 1 else 0 end)                                     AS invalid_cnt 
       ,SUM(case WHEN severity_level = '低' THEN 1 else 0 end)                                           AS low_cnt 
       ,SUM(case WHEN severity_level = '中' THEN 1 else 0 end)                                           AS medium_cnt 
       ,SUM(case WHEN severity_level = '高' THEN 1 else 0 end)                                           AS high_cnt 
       ,SUM(case WHEN severity_level = '严重' THEN 1 else 0 end)                                          AS critical_cnt 
       ,MIN(unix_timestamp(center_time) -unix_timestamp(event_time))/1000                                             AS min_druid -- 最小持续时间 
       ,MAX(unix_timestamp(center_time) -unix_timestamp(event_time))/1000                                             AS min_druid -- 最大持续时间 
       ,SUM(unix_timestamp(center_time) -unix_timestamp(event_time))/1000                                             AS min_druid -- 总大持续时间 
       ,FLOOR(AVG(unix_timestamp(center_time) - unix_timestamp(event_time)))/1000                                       AS avg_druid -- 平均持续时间 
       ,COUNT(distinct module_type)                                                                     AS module_type_uv 
       ,SUM(case WHEN module_type = 'safe' THEN 1 else null end)                                        AS safe_cnt
FROM ods.system_safe_attack_ips
WHERE stat_hour = '2020061618' 
GROUP BY  ip 
         ,facility_ip 
 