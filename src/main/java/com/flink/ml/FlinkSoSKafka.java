package com.flink.ml;


import com.flink.config.KafkaConfig;
import com.flink.feature.*;
import com.flink.udf.getDateDiffSecond;
import com.flink.udf.getDateFormat;
import com.flink.udf.getDateMin;
import com.flink.udf.getJsonObject;
import com.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Tumble.over;


public class FlinkSoSKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = ExecutionEnvUtil.prepare();
        streamEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        // Flink 读取 kafka 数据
        DataStreamSource<String> data = KafkaConfig.buildSource(streamEnv);
//        //处理加工逻辑  数据 加工

        SingleOutputStreamOperator<BaseSource> tempData = data.map(new FlinkFeatureService())
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

//        Table tableAll = tableEnv.fromDataStream(tempData,"content, eventTime.proctime, logType");

        Table table = tableEnv.fromDataStream(tempData, "logType,content,eventTime,rowTime.rowtime");
        tableEnv.registerTable("all_table", table);
        // register function
        tableEnv.createTemporarySystemFunction("getJsonObject", getJsonObject.class);
        tableEnv.createTemporarySystemFunction("getDateFormat", getDateFormat.class);
        tableEnv.createTemporarySystemFunction("getDateDiffSecond", getDateDiffSecond.class);
        tableEnv.createTemporarySystemFunction("getDateMin", getDateMin.class);
        // Table wordWithCount = tableEnv.sqlQuery("SELECT logType, count(eventTime) cnt FROM all_table group BY logType");

        StreamTable streamTable = new StreamTable(tableEnv);

        streamTable.registView();

//        Table processTab = tableEnv.sqlQuery("select * from audit_linuxserver_process_2020_06_30");
//        tableEnv.toRetractStream(processTab, Row.class).print();
//        String selectProcess = "SELECT  `user_name`                                             AS userid \n" +
//                "       ,`ip`                                                    AS login_ip -- 客户端id \n" +
//                "       ,`mac`                                                   AS mac -- 服务器ip \n" +
//                "       ,COUNT(`ip`)                                             AS pv \n" +
//                "       ,getDateMin(`center_time`)                            AS date_munit \n" +
//                "       ,MIN(getDateDiffSecond(`event_time`,`center_time`))        AS min_druid -- 最小持续时间 \n" +
//                "       ,MAX(getDateDiffSecond(`event_time`,`center_time`))        AS max_druid -- 最大持续时间 \n" +
//                "       ,FLOOR(AVG(getDateDiffSecond(`event_time`,`center_time`))) AS avg_druid -- 平均持续时间 \n" +
//                "       ,SUM(getDateDiffSecond(`event_time`,`center_time`))   AS sum_druid -- 总大持续时间 \n" +
//                "       ,COUNT(distinct `process`)                               AS process_uv -- 进程种类 \n" +
//                "       ,COUNT(distinct `ul_action`)                             AS ul_action_uv -- 类型种类 \n" +
//                "       ,COUNT(distinct `ul_style`)                              AS ul_style_uv -- 类型种类 \n" +
//                "       ,COUNT(distinct `ul_pid`)                                AS ul_pid_uv -- 类型种类 \n" +
//                "       ,COUNT(distinct `behaviour_type`)                        AS behaviour_type_uv -- 行为种类 \n" +
//                "       ,COUNT(distinct `event_level`)                           AS event_level_uv -- 行为种类 \n" +
//                "       ,AVG(cast(`size`                                AS int)) AS size\n" +
//                "FROM `process`\n" +
//                "GROUP BY  TUMBLE(rowTime, INTERVAL '10' SECOND)" +
//                "         ,`user_name` \n" +
//                "         ,`ip` \n" +
//                "         ,`mac` \n" +
//                "         ,getDateMin(`center_time`) ";
//
//        String selectFile = "SELECT  `user_name`                                             AS userid \n" +
//                "       ,`ip`                                                    AS login_ip -- 客户端id \n" +
//                "       ,`mac`                                                   AS mac -- 服务器ip \n" +
//                "       ,COUNT(`ip`)                                             AS pv \n" +
//                "       ,getDateMin(`center_time`)                            AS date_munit \n" +
//                "       ,MIN(getDateDiffSecond(`event_time`,`center_time`))        AS min_druid -- 最小持续时间 \n" +
//                "       ,MAX(getDateDiffSecond(`event_time`,`center_time`))        AS max_druid -- 最大持续时间 \n" +
//                "       ,FLOOR(AVG(getDateDiffSecond(`event_time`,`center_time`))) AS avg_druid -- 平均持续时间 \n" +
//                "       ,SUM(getDateDiffSecond(`event_time`,`center_time`))   AS sum_druid -- 总大持续时间 \n" +
//                "       ,COUNT(distinct `file_type`)                               AS file_type_uv -- 进程种类 \n" +
//                "       ,COUNT(distinct `file_or_dir_name`)                             AS file_or_dir_name_uv -- 类型种类 \n" +
//                "       ,COUNT(distinct `behaviour_type`)                              AS behaviour_type_uv -- 类型种类 \n" +
//                "       ,COUNT(distinct `operation_type`)                                AS operation_type_uv -- 类型种类 \n" +
//                "       ,AVG(cast(`size`                                AS int)) AS size\n" +
//                "FROM `file`\n" +
//                "GROUP BY  TUMBLE(rowTime, INTERVAL '10' SECOND)" +
//                "         ,`user_name` \n" +
//                "         ,`ip` \n" +
//                "         ,`mac` \n" +
//                "         ,getDateMin(`center_time`) ";

        String selectProcess = "select * from `process`";
        Table process = tableEnv.sqlQuery(selectProcess);
        process.execute().print();
//        TableResult process = tableEnv.executeSql(select);
//        tableEnv.toRetractStream(process, Row.class).print();
//        Table file = tableEnv.sqlQuery(selectFile);
////        tableEnv.toRetractStream(file, Row.class).print();
//        file.execute().print();


//        tableEnv.toRetractStream(wordWithCount, Row.class).print();
        //     tableEnv.toRetractStream(wordWithCount, Row.class).print();

//       tableEnv.sqlQuery("select logType,content,eventTime from all_table").execute().print();
//        Table wordWithCount = tableEnv.sqlQuery("SELECT logType, count(logType) FROM all_table GROUP BY logType");
//        tableEnv.toRetractStream(wordWithCount, Row.class).print();


//        Table windowCnt = tableEnv.sqlQuery("SELECT TUMBLE_START(rowTime, INTERVAL '10' SECOND) AS window_start," +
//                "TUMBLE_END(rowTime, INTERVAL '10' SECOND) AS window_end, count(logType),logType FROM all_table" +
//                " GROUP BY TUMBLE(rowTime, INTERVAL '10' SECOND),logType");
//        windowCnt.execute().print();
        // tableEnv.createTemporaryView("windowCnt", windowCnt);

        //   tableEnv.toRetractStream(windowCnt, Row.class).print();


        streamEnv.execute("Blink Stream SQL Job");
    }

}
