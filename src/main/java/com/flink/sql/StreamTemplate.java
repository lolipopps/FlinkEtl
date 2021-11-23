package com.flink.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;


public class StreamTemplate {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Long> dataStream = env.fromCollection(Arrays.asList(1L, 2L, 3L, 4L));
        tableEnv.registerDataStream("test", dataStream);
        Table result = tableEnv.sqlQuery("SELECT * FROM test ");
        tableEnv.toAppendStream(result, Row.class).print();
        env.execute();
    }
}
