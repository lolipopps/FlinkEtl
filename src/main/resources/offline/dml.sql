DROP TABLE stg.system_safe_operation_auth;
CREATE EXTERNAL TABLE IF NOT EXISTS stg.system_safe_operation_auth ( manufacturers_facility string ,module_type string ,user_name string ,log_level float ,event_title string ,login_result string ,priority float ,facility_ip string ,login_ip string ,manufacturers_name string ,auth_method string ,log_type string ,event_type string ,asset_name string ,log_des string ,system_type string ,user_id string ,facility_type string ,facility float ,center_time string ,event_time timestamp ) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES( 'es.resource'='system-safe-operation_auth-2020-06-17*/doc', 'es.nodes'='kafka:9810' );

SELECT  *
FROM system_safe_operation_auth
LIMIT 1;

DROP TABLE stg.system_safe_attack_ips;
CREATE EXTERNAL TABLE IF NOT EXISTS stg.system_safe_attack_ips ( event_son_type string ,attack_type string ,manufacturers_facility string ,facility_hostname string ,threat_id string ,hit_direction string ,protection string ,zone_name string ,severity_level string ,sub_protection string ,log_type string ,cve string ,event_type string ,attack_sub_category string ,app_protocol string ,dest_zone_name string ,facility_type string ,conduct_operations string ,center_time string ,dest_port float ,module_type string ,ip string ,log_level float ,policy_name string ,abstract string ,priority float ,facility_ip string ,manufacturers_name string ,protocol_type string ,threat_name string ,log_des string ,port float ,system_type string ,vpn string ,dest_ip string ,bid string ,facility float ,msb string ,event_time timestamp ) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES( 'es.resource'='system-safe-attack_ips-2020-06-17*/doc', 'es.nodes'='kafka:9810' );

SELECT  *
FROM system_safe_attack_ips
LIMIT 1;

DROP TABLE stg.audit_linuxserver_process;
CREATE EXTERNAL TABLE IF NOT EXISTS stg.audit_linuxserver_process ( user_name string ,mac string ,behaviour_type bigint ,log_type string ,center_time string ,event_sender string ,event_source_id string ,process string ,module_type string ,ul_pid bigint ,ip string ,end_time string ,event_local_time string ,cp_path_name string ,paramerter string ,start_time string ,center_key string ,event_id string ,ul_memory_value string ,size bigint ,system_type string ,event_level bigint ,ul_action bigint ,ul_style bigint ,event_time timestamp ) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES( 'es.resource'='audit-linuxserver-process-2020-06-30*/doc', 'es.nodes'='kafka:9810' );

SELECT  *
FROM audit_linuxserver_process
LIMIT 1;

DROP TABLE stg.audit_app_otp_user;
CREATE EXTERNAL TABLE stg.audit_app_otp_user ( logcontent string ,userid string ,domainid string ,hostname string ,log_type string ,event_type string ,clientip string ,moduletype string ,actionid string ,serverip string ,center_time string ,actionresult string ,module_type string ,log_level bigint ,vendorid string ,priority bigint ,domianname string ,facility_ip string ,token string ,app_name string ,manufacturers_name string ,system_type string ,orgunitid string ,facility bigint ,logtime string ,event_time timestamp ) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES( 'es.resource'='audit-app-otp_user-2020-07-07*/doc', 'es.nodes'='kafka:9810' );

SELECT  *
FROM stg.audit_app_otp_user
LIMIT 1;

DROP TABLE stg.audit_linuxserver_abnormalprogress;
CREATE EXTERNAL TABLE IF NOT EXISTS stg.audit_linuxserver_abnormalprogress ( event_sender string ,event_source_id string ,per_use bigint ,module_type string ,user_name string ,ip string ,event_local_time string ,c_reserve string ,type bigint ,mac string ,behaviour_type bigint ,progress_name string ,center_key string ,log_type string ,total bigint ,event_id string ,size bigint ,system_type string ,event_level bigint ,free bigint ,per_time bigint ,center_time string ,event_time timestamp ,limit_use bigint ) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES( 'es.resource'='audit-linuxserver-abnormalprogress-2020-06-28*/doc', 'es.nodes'='kafka:9810' );

SELECT  *
FROM audit_linuxserver_abnormalprogress
LIMIT 1;

DROP TABLE stg.audit_linuxserver_address;
CREATE EXTERNAL TABLE IF NOT EXISTS stg.audit_linuxserver_address( event_sender string ,event_source_id string ,module_type string ,user_name string ,ip string ,event_local_time string ,mac string ,behaviour_type bigint ,center_key string ,log_type string ,event_id string ,size bigint ,system_type string ,dest_ip string ,event_level bigint ,style bigint ,center_time string ,event_time timestamp ) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES( 'es.resource'='audit-linuxserver-address-2020-06-24*/doc', 'es.nodes'='kafka:9810' );

SELECT  *
FROM audit_linuxserver_address
LIMIT 1;

DROP TABLE stg.audit_linuxserver_file;
CREATE EXTERNAL TABLE IF NOT EXISTS stg.audit_linuxserver_file ( event_sender string ,event_source_id string ,operation_type bigint ,module_type string ,user_name string ,ip string ,event_local_time string ,mac string ,behaviour_type bigint ,center_key string ,log_type string ,event_id string ,size bigint ,system_type string ,file_type bigint ,event_level bigint ,center_time string ,event_time timestamp ,file_or_dir_name string ) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES( 'es.resource'='audit-linuxserver-file-2020-06-30*/doc', 'es.nodes'='kafka:9810' );

SELECT  *
FROM audit_linuxserver_file
LIMIT 1;

DROP TABLE stg.audit_linuxserver_network;
CREATE EXTERNAL TABLE IF NOT EXISTS stg.audit_linuxserver_network( event_sender string ,event_source_id string ,module_type string ,local_port bigint ,user_name string ,ip string ,remote_port bigint ,event_local_time string ,protocal_type bigint ,mac string ,center_key string ,domain_name string ,log_type string ,protocol string ,event_id string ,remote_ip string ,size bigint ,system_type string ,process_name string ,event_level bigint ,style bigint ,center_time string ,event_time timestamp ) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' TBLPROPERTIES( 'es.resource'='audit-linuxserver-network-2020-06*/doc', 'es.nodes'='kafka:9810' );

SELECT  *
FROM stg.audit_linuxserver_network
LIMIT 1; 