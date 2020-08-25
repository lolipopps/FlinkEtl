package com.flink.ml;


import avro.shaded.com.google.common.collect.Lists;
import com.alibaba.alink.pipeline.classification.GbdtClassifier;
import com.flink.config.KafkaConfig;
import com.flink.config.PropertiesConstants;
import com.flink.ml.feature.AdultTrain;
import com.flink.ml.model.Model;
import com.flink.ml.model.ModelGrid;
import com.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class FlinkSoS {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment batchEnv = ExecutionEnvUtil.prepare();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(batchEnv);
        String version = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.HIVE_VERSION);
        String name = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.HIVE_NAME);
        String dbName = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.HIVE_DBNAME);
        String hiveConfDir = ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SYS_CONFPATH);
        HiveCatalog hive = new HiveCatalog(name, dbName, hiveConfDir, version);
        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");
        String[] functions = tableEnv.listFunctions();
        Arrays.stream(functions).forEach(System.out::println);


        String query = "SELECT  user_name                                                                                                                                     AS userid \n" +
                "       ,ip                                                                                                                                            AS login_ip -- 客户端id \n" +
                "       ,mac                                                                                                                                           AS mac -- 服务器ip \n" +
                "       ,COUNT(ip)                                                                                                                                     AS pv \n" +
                "       ,CONCAT(substr(regexp_replace(center_time,'T',' '),1,15) ,'0')                                                                   AS date_munit \n" +
                "FROM audit_linuxserver_process o\n" +
                "GROUP BY  TUMBLE(event_time,INTERVAL '1' DAY) ,user_name \n" +
                "         ,ip \n" +
                "         ,mac \n" +
                "         ,CONCAT(substr(regexp_replace(center_time,'T',' '),1,15) ,'0')";

        TableResult tableResult2 = tableEnv.executeSql(query);

        try (CloseableIterator<Row> it = tableResult2.collect()) {
            while (it.hasNext()) {
                Row row = it.next();
                System.out.println(row);
            }
        }



    }

}
