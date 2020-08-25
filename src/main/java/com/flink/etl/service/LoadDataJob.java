package com.flink.etl.service;

import com.flink.config.HiveConfig;
import com.flink.config.PropertiesConstants;
import com.flink.hive.HiveDB;
import com.flink.util.DateUtil;
import com.flink.util.ExecutionEnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.List;

@Slf4j

public class LoadDataJob implements Runnable {
    @Override
    public void run() {
        log.info("开始加载分区 和合并数据");
        HiveDB hiveDB = HiveConfig.getHiveDB();

        List<String> tables = null;
        try {
            tables = hiveDB.listTableNames();
        } catch (DatabaseNotExistException e) {
            log.info("listTableNames error", e.getMessage());
        }

        StringBuilder loadSql = new StringBuilder();

        for (String table : tables) {
            for (int i = 0; i < 6; i++) {
                loadSql.append("load data inpath '" + ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.HIVE_HDFSPATH) + table + "/pt=" + DateUtil.getBeforeHour() + i + "0/*' into table " + table + " partition(pt='" + DateUtil.getBeforeHour() + i + "0');");
            }
        }
        log.info(loadSql.toString());
        try {
            hiveDB.execute(loadSql.toString());
        } catch (Exception e) {
            log.info("加载分区失败: ",e.getMessage());
        }
    }
}