#!/bin/bash
"""
执行方式 sh -x dml.sh /root/data/sql
参数 sql 
"""
stat_date=$(date +%Y-%m-%d)
line_stat_date=-${stat_date}
stat_month=$(date +%Y-%m)
line_stat_month=-${stat_month}
stat_hour=$(date +%Y%m%d%H)
sqlPath=$1
hive -hiveconf stat_date=${line_stat_date} -f ${sqlPath}/dml.sql
hive -hiveconf stat_date=${line_stat_date} -f ${sqlPath}/dmlpart.sql