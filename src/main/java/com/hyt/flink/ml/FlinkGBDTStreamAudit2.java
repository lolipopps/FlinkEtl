package com.hyt.flink.ml;


import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.hyt.flink.config.KafkaConfig;
import com.hyt.flink.feature.*;
import com.hyt.flink.ml.model.Model;
import com.hyt.flink.udf.*;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;


public class FlinkGBDTStreamAudit2 {

    public static void main(String[] args) throws Exception {
        MLEnvironment mlEnvironment = new MLEnvironment();
        StreamExecutionEnvironment streamEnv = mlEnvironment.getStreamExecutionEnvironment();
        StreamTableEnvironment tableEnv = mlEnvironment.getStreamTableEnvironment();
        streamEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties props = KafkaConfig.buildKafkaProps();
        FlinkKafkaConsumer011<String> allData = new FlinkKafkaConsumer011<>(
                "flinkEtl",
                new SimpleStringSchema(),
                props);
        DataStreamSource<String> res = streamEnv.addSource(allData);
        SingleOutputStreamOperator<BaseSource> datas = res.map(new FlinkFeatureService1())
                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());
        Table table = tableEnv.fromDataStream(datas, "logType,content,eventTime,rowTime.rowtime");
        tableEnv.registerTable("all_table", table);
        // register function
        tableEnv.createTemporarySystemFunction("getJsonObject", getJsonObject.class);
        tableEnv.createTemporarySystemFunction("getLineObject", getLineObject.class);
        tableEnv.createTemporarySystemFunction("getDateFormat", getDateFormat.class);
        tableEnv.createTemporarySystemFunction("getDateDiffSecond", getDateDiffSecond.class);
        tableEnv.createTemporarySystemFunction("getDateMin", getDateMin.class);
        StreamTableFeature streamTableFeature = new StreamTableFeature(tableEnv, "./sql/audit.sql");
        Table tables = tableEnv.sqlQuery("select * from `audit_train`");
        TableSourceStreamOp tableSourceStreamOp = new TableSourceStreamOp(tables);
        Model model = new Model();
        model.predict(tableSourceStreamOp);
        streamEnv.execute("Blink Stream SQL Job");

    }
}
