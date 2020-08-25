package com.flink.config;

import com.flink.hive.HiveDB;
import com.flink.util.ExecutionEnvUtil;
import lombok.Data;
import org.apache.flink.table.catalog.hive.HiveCatalog;

@Data
public class HiveConfig {

    public static HiveCatalog getHiveCatalog() {

        String name = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.HIVE_NAME);
        String dbName = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.HIVE_DBNAME);
        String hiveConfDir = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SYS_CONFPATH);
        String version = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.HIVE_VERSION);
        return new HiveCatalog(name, dbName, hiveConfDir, version);

    }

    public static HiveDB getHiveDB() {

        String name = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.HIVE_NAME);
        String dbName = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.HIVE_DBNAME);
        String hiveConfDir = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SYS_CONFPATH);
        String version = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.HIVE_VERSION);
        return new HiveDB(hiveConfDir, version, name);

    }


}
