package com.flink.sqlapi;


import com.flink.bean.Person;
import com.flink.sql.PrepareData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.List;


public class DistinctTemplate {



    @Test
    public void testDataSet() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);


        List<Person> clicksData = PrepareData.getPersonData();
        DataSet<Person> dataStream = env.fromCollection(clicksData);

        tableEnv.registerDataSet("Person", dataStream, "name,age,city");
        Table table = tableEnv.sqlQuery("SELECT DISTINCT city FROM Person");

        DataSet<Row> result = tableEnv.toDataSet(table, Row.class);
        result.print();
    }


    @Test
    public void testDataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        List<Person> clicksData = PrepareData.getPersonData();
        DataStream<Person> dataStream = env.fromCollection(clicksData);

        tableEnv.registerDataStream("Person", dataStream, "name,age,city");

        Table table = tableEnv.sqlQuery("SELECT DISTINCT city FROM Person");
        tableEnv.toRetractStream(table, Row.class).print();
        env.execute();
    }


}
