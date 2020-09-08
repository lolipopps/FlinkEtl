package com.flink.ml;
import com.alibaba.alink.operator.stream.source.MySqlSourceStreamOp;
import com.flink.util.ExecutionEnvUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSoS {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = ExecutionEnvUtil.prepare();
        streamEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

    }

}
