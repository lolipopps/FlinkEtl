package com.flink.ml;


import com.flink.config.KafkaConfig;
import com.flink.feature.BaseSourceTable;
import com.flink.feature.FlinkFeatureService;
import com.flink.util.ExecutionEnvUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class FlinkSoSKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = ExecutionEnvUtil.prepare();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        // Flink 读取 kafka 数据
        DataStreamSource<String> data = KafkaConfig.buildSource(streamEnv);
        //处理加工逻辑  数据 加工
        SingleOutputStreamOperator<BaseSourceTable> tempData = data.map(new FlinkFeatureService());
        Table tableAll = tableEnv.fromDataStream(tempData);


    }

}
