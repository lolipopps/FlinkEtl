package com.hyt.flink.config;

import com.alibaba.alink.hive.HiveDB;
import com.hyt.flink.util.ExecutionEnvUtil;
import lombok.Data;
import org.apache.flink.table.catalog.hive.HiveCatalog;

@Data
public class HiveConfig {

    static public String name = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.HIVE_NAME);
    static public String dbName = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.HIVE_DBNAME);
    static public String hiveConfDir = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SYS_CONFPATH);
    static public String version = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.HIVE_VERSION);

    public static HiveCatalog getHiveCatalog() {
        return new HiveCatalog(name, dbName, hiveConfDir, version);

    }

    public static HiveDB getHiveDB() {
        return new HiveDB(hiveConfDir, version, name);

    }


}
