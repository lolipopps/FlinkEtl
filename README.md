# FlinkEtl
FlinkEtl

该程序设计实时数据采集流批一体的方案对数据进行实时清洗和预测

环境
hadoop-2.9.2  hive-2.4.4  kafka-1.11 flink-1.11 alink-1.11_2.11-1.2.0 elastic-6.8.3 mysql5.7

首先搭建好hadoop 集群 装上 hive 配置好 hive 读取es数据的功能
开启hiveserver2 metastore 服务

离线部分(加工特征) 
见 offline 文件夹的sql 文件采用 shell 脚本调度 sql 文件的方式

1: dml.sh  参数1 sql的路径     
功能 表结构定义 对应两个 sql dml.sql 以及 dmlpart.sql

2: runSql.sh 参数1 sql的路径     
生成小时段特征 (定时调度)