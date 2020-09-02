CREATE VIEW  process_cnt AS
SELECT  `user_name`                                               AS userid -- 用户id
       ,`ip`                                                      AS login_ip -- 客户端id
       ,`mac`                                                     AS mac -- 服务器ip
       ,COUNT(`ip`)                                               AS pv -- pv 数量
       ,getDateMIN(`center_time`)                                 AS date_munit
       ,MIN(getDateDiffSecond(`event_time`,`center_time`))        AS min_druid -- 最小持续时间
       ,MAX(getDateDiffSecond(`event_time`,`center_time`))        AS max_druid -- 最大持续时间
       ,FLOOR(AVG(getDateDiffSecond(`event_time`,`center_time`))) AS avg_druid -- 平均持续时间
       ,SUM(getDateDiffSecond(`event_time`,`center_time`))        AS sum_druid -- 总大持续时间
       ,COUNT(distinct `process`)                                 AS process_uv -- 进程种类
       ,COUNT(distinct `ul_action`)                               AS ul_action_uv -- 类型种类
       ,COUNT(distinct `ul_style`)                                AS ul_style_uv -- 类型种类
       ,COUNT(distinct `ul_pid`)                                  AS ul_pid_uv -- 类型种类
       ,COUNT(distinct `behaviour_type`)                          AS behaviour_type_uv -- 行为种类
       ,COUNT(distinct `event_level`)                             AS event_level_uv -- 行为种类
       ,AVG(cast(`size`                                  AS int)) AS size
FROM `process`
GROUP BY  `user_name`
         ,`ip`
         ,`mac`
         ,getDateMIN(`center_time`);


CREATE VIEW  file_cnt AS
SELECT  `user_name`                                             AS user_id
     ,`ip`                                                    AS login_ip -- 客户端id
     ,`mac`                                                   AS mac -- 服务器ip
     ,COUNT(`ip`)                                             AS all_pv
     ,getDateMin(`center_time`)                            AS date_munit
     ,MIN(getDateDiffSecond(`event_time`,`center_time`))        AS min_druid -- 最小持续时间
     ,MAX(getDateDiffSecond(`event_time`,`center_time`))        AS max_druid -- 最大持续时间
     ,FLOOR(AVG(getDateDiffSecond(`event_time`,`center_time`))) AS avg_druid -- 平均持续时间
     ,SUM(getDateDiffSecond(`event_time`,`center_time`))   AS sum_druid -- 总大持续时间
     ,COUNT(distinct `file_type`)                               AS file_type_uv -- 进程种类
     ,COUNT(distinct `file_or_dir_name`)                             AS file_or_dir_name_uv -- 类型种类
     ,COUNT(distinct `behaviour_type`)                              AS behaviour_type_uv -- 类型种类
     ,COUNT(distinct `operation_type`)                                AS operation_type_uv -- 类型种类
     ,AVG(cast(`size`                                AS int)) AS size
FROM `file`
GROUP BY `user_name`
        ,`ip`
        ,`mac`
        ,getDateMin(`center_time`);