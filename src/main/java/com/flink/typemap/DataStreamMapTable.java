package com.flink.typemap;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Arrays;


public class DataStreamMapTable {


    @Test
    public void dataStreamConvertTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple3<Long, String, Double>> order = env.fromCollection(Arrays.asList(
                new Tuple3<Long, String, Double>(1L, "手机", 1899.00),
                new Tuple3<Long, String, Double>(1L, "电脑", 8888.00),
                new Tuple3<Long, String, Double>(3L, "平板", 899.99)));

        Table tableObject = tableEnv.fromDataStream(order, "id,name,amount");

        tableEnv.registerTable("table_order", tableObject);

        Table result = tableEnv.sqlQuery("SELECT * FROM table_order WHERE amount < 1000");

        tableEnv.toAppendStream(result, Row.class).print("Sql");

        result = tableEnv.sqlQuery("SELECT name FROM " + tableObject + " WHERE amount < 2000");
        tableEnv.toAppendStream(result, Row.class).print("Table");
        env.execute();
    }


    @Test
    public void dataStreamRegisterTable() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStream<Tuple2<Long, String>> order = env.fromCollection(Arrays.asList(
                new Tuple2<Long, String>(1L, "手机"),
                new Tuple2<Long, String>(1L, "电脑"),
                new Tuple2<Long, String>(3L, "平板")));

        tableEnv.registerDataStream("table_order", order);

        Table result = tableEnv.sqlQuery("SELECT * FROM table_order WHERE f0 < 3");

        tableEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }


}