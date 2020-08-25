hive --service hiveserver2 > /root/logs/hiveserver.log 2>&1 &
hive --service metastore  > /root/logs/metastore.log 2>&1 &

kafka-topics.sh  --delete --zookeeper kafka:2181  --topic flink

kafka-topics.sh --create --zookeeper kafka:2181 --replication-factor 1 --partitions 6 --topic flinkEtl

kafka-topics.sh --describe --zookeeper kafka:2181 --topic flinkEtl


kafka-console-consumer.sh --bootstrap-server kafka:9092  --topic flinkEtl

规则配置
1	system_safe_attack_apt	系统apt攻击	safe	record_time,timestamp,sensor_name,sensor_ip,event_source,uuid,inter,rid,classtype,category,sub_category,src_ip,src_port,src_service,dst_ip,dst_port,dst_service,proto,app_proto,kill_chain,payload,resp_data,severity,reliability,fam,target,desc,message,src_ip_country,src_ip_city,src_ip_geoloc,src_ip_country_code,dst_ip_country,dst_ip_city,dst_ip_geoloc,dst_ip_country_code	\\t	string,string,string,string,string,string,string,int,string,string,string,ipaddr,int,string,string,int,string,string,string,string,string,string,int,int,string,string,string,string,string,string,string,string,string,string,string,string	记录时间,时间戳,设备主机名,设备IP,数据来源,事件uuid,流量监测网卡,事件规则ID,威胁大类,威胁类别,威胁子类别,源IP,源端口,源IP服务,目的IP,目的端口,目的IP服务,传输层协议,应用层协议,事件攻击链位置,攻击/异常载荷,攻击返回数据,威胁严重性,事件可信度,威胁所在家族,威胁对象,威胁描述,详细信息,源IP所在国家名,源IP所在城市名,源IP经纬度,源IP国家代码,目的IP所在国家名,目的IP所在城市名,目的IP经纬度,目的IP国家代码		apt:	system_safe_attack_apt
CREATE  TABLE `patitionTable`(
  `dept_no` int,
  `addr` string,
  `tel` string)
partitioned by(code string,pt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH  SERDEPROPERTIES ("field.delim"="$$")
1$$$$hyt
$$abc$$dd
$$$$$$
1$$$$
hadoop  fs -mkdir -p hdfs://hadoop:9000/hive/warehouse/flink.db/patitiontable/code=huasN/pt=20200805000000
hadoop  fs -put 2.txt hdfs://hadoop:9000/hive/warehouse/flink.db/patitiontable/code=huasN/pt=20200805000000

load data inpath 'hdfs://hadoop:9000/hive/warehouse/flink.db/patitiontable/code=huasN/pt=20200805000000' into table patitiontable partition(code='huasN',pt='20200805000000');

CREATE TABLE system_safe_attack_apt(
record_time string,
timestamp_time string,
sensor_name string,
sensor_ip string,
event_source string,
uuid string,
inter string,
rid int,
classtype string,
category string,
sub_category string,
src_ip string,
src_port int,
src_service string,
dst_ip string,
dst_port int,
dst_service string,
proto string,
app_proto string,
kill_chain string,
payload string,
resp_data string,
severity int,
reliability int,
fam string,
target string,
desc string,
message string,
src_ip_country string,
src_ip_city string,
src_ip_geoloc string,
src_ip_country_code string,
dst_ip_country string,
dst_ip_city string,
dst_ip_geoloc string,
dst_ip_country_code string)
partitioned by(pt string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ("field.delim"="$$")

load data inpath 'hdfs://hadoop:9000/hive/warehouse/flink.db/system_safe_attack_apt/pt=202008081410' into table system_safe_attack_apt partition(pt='202008081410');

add jar /root/app/hive-2.3.7/lib/hive-contrib-2.3.7.jar



flink run -m yarn-cluster -yn 2 ./examples/batch/WordCount.jar
./bin/flink run -m yarn-cluster   n FlinkEtl.jar
./bin/flink run -m yarn-cluster -p 6 -yjm 1024m -ytm 4096m  -c com.flink.etl.FlinkEtl FlinkEtl-1.0-SNAPSHOT-jar-with-dependencies.jar
./bin/flink run -m yarn-cluster -yn 6 -yjm 1024 -ytm 1024 -d  -c com.flink.ml.FlinkMlTrain FlinkEtl-1.0-SNAPSHOT-jar-with-dependencies.jar
./bin/flink run -m yarn-cluster -p 6 -yjm 1024m -ytm 4096m  -c com.flink.ml.FlinkMlTrain ../../data/FlinkEtl-1.0-SNAPSHOT-jar-with-dependencies.jar

export HADOOP_CLASSPATH=`hadoop classpath`