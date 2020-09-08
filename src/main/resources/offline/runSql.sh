#!/bin/bash
"""
执行方式 sh -x runSql.sh /root/data/sql/run
参数 sql 定义语句的路径
"""
stat_date=`date +%Y-%m-%d`
line_stat_date=-${stat_date}
stat_month=`date +%Y-%m`
line_stat_month=-${stat_month}
stat_hour=`date +%Y%m%d%H`
path=$1
files=`ls ${path}`
for file in ${files}
do
	   hive -hiveconf stat_date=${line_stat_date} stat_month=${line_stat_date} stat_date=${line_stat_date} -f ${path}/${file}
done