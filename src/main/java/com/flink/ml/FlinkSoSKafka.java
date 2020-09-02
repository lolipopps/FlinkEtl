package com.flink.ml;

import com.flink.config.KafkaConfig;
import com.flink.config.PropertiesConstants;
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
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.HashMap;

public class FlinkSoSKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = ExecutionEnvUtil.prepare();
        streamEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);


        // Flink 读取 kafka 数据
        DataStream<String> allData = null;
        for (String topic : ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.SOURCE_TOPIC).split(",")) {
            if (allData == null) {
                allData = KafkaConfig.buildSource(streamEnv, topic);
            } else {
                allData =  allData.union(KafkaConfig.buildSource(streamEnv, topic));
            }

        }
        SingleOutputStreamOperator<BaseSource> datas = allData.map(new FlinkFeatureService())
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

        Table table = tableEnv.fromDataStream(datas, "logType,content,eventTime,rowTime.rowtime");
        tableEnv.registerTable("all_table", table);
        // register function
        tableEnv.createTemporarySystemFunction("getJsonObject", getJsonObject.class);
        tableEnv.createTemporarySystemFunction("getDateFormat", getDateFormat.class);
        tableEnv.createTemporarySystemFunction("getDateDiffSecond", getDateDiffSecond.class);
        tableEnv.createTemporarySystemFunction("getDateMin", getDateMin.class);
//        Table wordWithCount = tableEnv.sqlQuery("SELECT logType, count(eventTime) cnt FROM all_table group BY logType");
//        tableEnv.toRetractStream(wordWithCount, Row.class).print();
        StreamSourceTable streamSourceTable = new StreamSourceTable(tableEnv);
        StreamTableFeature streamTableFeature = new StreamTableFeature(tableEnv);
        StreamSinkTable streamSinkTable = new StreamSinkTable(tableEnv);
        streamSinkTable.printRegistTable();
        tableEnv.sqlQuery("select * from `process_cnt`").execute().print();
        streamEnv.execute("Blink Stream SQL Job");
    }

}
