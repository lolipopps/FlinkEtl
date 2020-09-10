package com.hyt.flink.ml;


import com.alibaba.alink.common.MLEnvironment;
import com.alibaba.alink.common.MLEnvironmentFactory;
import com.alibaba.alink.kafka.Kafka011SourceStreamOp;
import com.alibaba.alink.kafka.MyKafka011SourceStreamOp;
import com.alibaba.alink.operator.batch.BatchOperator;
import com.alibaba.alink.operator.stream.StreamOperator;
import com.alibaba.alink.operator.stream.source.TableSourceStreamOp;
import com.hyt.flink.config.KafkaConfig;
import com.hyt.flink.config.PropertiesConstants;
import com.hyt.flink.feature.*;
import com.hyt.flink.ml.feature.BaseData;
import com.hyt.flink.ml.model.Model;
import com.hyt.flink.udf.*;
import com.hyt.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.hyt.flink.ml.feature.TableToBaseDataUtils.toBaseData;


public class FlinkGBDTStreamAudit {

    public static void main(String[] args) throws Exception {
        // Flink 读取 kafka 数据
        String topicName = "flinkEtl";
        MyKafka011SourceStreamOp data = new MyKafka011SourceStreamOp()
                .setBootstrapServers(ExecutionEnvUtil.PARAMETER_TOOL.get(PropertiesConstants.KAFKA_BROKERS))
                .setTopic(topicName)
                .setStartupMode("LATEST")
                .setGroupId("alink_group");
        System.out.println(data.getSchema());
        Table table = data.select("message as content").getOutputTable();
        StreamTableEnvironment tableEnv = MLEnvironmentFactory.get(data.getMLEnvironmentId()).getStreamTableEnvironment();
        StreamExecutionEnvironment env = MLEnvironmentFactory.get(data.getMLEnvironmentId()).getStreamExecutionEnvironment();

        tableEnv.registerTable("all_table", table);
        tableEnv.createTemporarySystemFunction("getJsonObject", getJsonObject.class);
        tableEnv.createTemporarySystemFunction("getLineObject", getLineObject.class);
        tableEnv.createTemporarySystemFunction("getDateFormat", getDateFormat.class);
        tableEnv.createTemporarySystemFunction("getDateDiffSecond", getDateDiffSecond.class);
        tableEnv.createTemporarySystemFunction("getDateMin", getDateMin.class);
        StreamTableFeature streamTableFeature = new StreamTableFeature(tableEnv, "./sql/audit.sql");
        Table resTable = tableEnv.sqlQuery("select * from audit_train");
        TableSourceStreamOp tableSourceStreamOp = new TableSourceStreamOp(resTable);
        Model model = new Model();
        model.predict(tableSourceStreamOp);
    }
}
