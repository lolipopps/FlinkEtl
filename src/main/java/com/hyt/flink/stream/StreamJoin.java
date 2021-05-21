package com.hyt.flink.stream;
import com.hyt.flink.config.KafkaConfig;
import com.hyt.flink.config.PropertiesConstants;
import com.hyt.flink.feature.*;
import com.hyt.flink.udf.getDateDiffSecond;
import com.hyt.flink.udf.getDateFormat;
import com.hyt.flink.udf.getDateMin;
import com.hyt.flink.udf.getJsonObject;
import com.hyt.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.ArrayList;
import java.util.List;

public class StreamJoin {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamEnv = ExecutionEnvUtil.prepare();
        streamEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        // Flink 读取 kafka 数据
        DataStreamSource<String> streamA = KafkaConfig.buildSource(streamEnv, "streamA");

        DataStreamSource<String> streamB = KafkaConfig.buildSource(streamEnv, "streamB");

        SingleOutputStreamOperator<BaseSource> datas = streamA.map(new FlinkFeatureService())
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());

        Table table = tableEnv.fromDataStream(datas, "logType,content,eventTime,rowTime.rowtime");
        tableEnv.registerTable("all_table", table);
        // register function
        tableEnv.createTemporarySystemFunction("getJsonObject", getJsonObject.class);
        tableEnv.createTemporarySystemFunction("getDateFormat", getDateFormat.class);
        tableEnv.createTemporarySystemFunction("getDateDiffSecond", getDateDiffSecond.class);
        tableEnv.createTemporarySystemFunction("getDateMin", getDateMin.class);
        StreamSourceTable streamSourceTable = new StreamSourceTable(tableEnv);
        StreamTableFeature streamTableFeature = new StreamTableFeature(tableEnv);
        StreamSinkTable streamSinkTable = new StreamSinkTable(tableEnv);
        streamSinkTable.printRegistTable();
        tableEnv.sqlQuery("select * from `process_cnt`").execute().print();
        streamEnv.execute("Blink Stream SQL Job");
    }

}
