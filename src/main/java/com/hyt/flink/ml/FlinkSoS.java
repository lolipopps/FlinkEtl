package com.hyt.flink.ml;


import com.hyt.flink.config.HiveConfig;
import com.alibaba.alink.hive.operator.batch.HiveSourceBatchOp;
import com.hyt.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkSoS {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment batchEnv = ExecutionEnvUtil.prepareBatch();
        HiveSourceBatchOp data = new HiveSourceBatchOp()
                .setInputTableName("audit_linuxserver_abnormalprogres_2020062900")
//                .setPartitions("stat_hour=2020062900")
                .setHiveVersion(HiveConfig.version)
                .setHiveConfDir(HiveConfig.hiveConfDir)
                .setDbName(HiveConfig.dbName);
        System.out.println(data.getColNames());

    }

}
