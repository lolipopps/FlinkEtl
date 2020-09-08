package com.hyt.flink.etl.service;

import com.hyt.flink.config.HiveConfig;
import com.hyt.flink.config.PropertiesConstants;
import com.alibaba.alink.hive.HiveDB;
import com.hyt.flink.util.DateUtil;
import com.hyt.flink.util.ExecutionEnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;

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
        } catch (Exception e) {
            e.printStackTrace();
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