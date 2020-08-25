package com.flink.ml;


import com.flink.config.KafkaConfig;
import com.flink.feature.BaseSource;
import com.flink.feature.FlinkFeatureService;
import com.flink.feature.TimestampsAndWatermarks;
import com.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class FlinkSoSTmp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = ExecutionEnvUtil.prepare();
        streamEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
        // Flink 读取 kafka 数据
        DataStreamSource<String> data = KafkaConfig.buildSource(streamEnv);
//        //处理加工逻辑  数据 加工

//        SingleOutputStreamOperator<BaseSource> tempData = data.map(new FlinkFeatureService())
//                .assignTimestampsAndWatermarks(new TimestampsAndWatermarks());
//        ;
//        Table tableAll = tableEnv.fromDataStream(tempData,"content, eventTime.proctime, logType");
        SingleOutputStreamOperator<BaseSource> tempData = data.map(new FlinkFeatureService()).setParallelism(2);
        Table tableAll = tableEnv.fromDataStream(tempData,"logType,content,eventTime,rowTime.rowtime");
        tableEnv.registerTable("all_table", tableAll);
        tableEnv.sqlQuery("select logType,content,eventTime,rowTime from all_table").execute().print();

    }

}
