package com.flink.config;
public class PropertiesConstants {
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final String KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect";
    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    public static final String DEFAULT_KAFKA_GROUP_ID = "kafka.group.id";
    public static final String SOURCE_TOPIC = "kafka.source.topic";
    public static final String SINK_TOPIC = "kafka.sink.topic";
    public static final String CONSUMER_FROM_TIME = "consumer.from.time";
    public static final String STREAM_SINK_PARALLELISM = "stream.sink.parallelism";
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_DIR = "stream.checkpoint.dir";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
    public static final String PROPERTIES_FILE_NAME = "application.properties";
    public static final String MYSQL_URL = "mysql.url";
    public static final String MYSQL_PASSWORD = "mysql.password";
    public static final String MYSQL_USERNAME = "mysql.username";
    public static final String HIVE_HDFSPATH = "hive.hdfsPath";
    public static final String HIVE_DBNAME = "hive.dbName";
    public static final String HIVE_NAME = "hive.name";
    public static final String HIVE_VERSION = "hive.version";
    public static final String SYS_CONFPATH = "sys.confpath";
    public static final String SYS_SQLPATH = "sys.sqlpath";
    public static final String SYS_MODELPATH = "sys.modelpath";
    public static final String ELASTIC_PORT = "elastic.port";
    public static final String ELASTIC_HOSTNAME = "elastic.hostname";
    public static final String HDFS_FILESIZE = "hdfs.filesize";
    public static final String FEATURE_SQL_FILE = "./sql/windowsFeature.sql";
    public static final String SINK_SQL_FILE = "./sql/result.sql";


}
