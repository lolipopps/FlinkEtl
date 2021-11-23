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


public class UnionAllTemplate {



    @Test
    public void testDataSet() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        List<Person> personData = PrepareData.getPersonData();
        DataSet<Person> dataStream = env.fromCollection(personData);

        tEnv.registerDataSet("Person", dataStream, "name,age,city");

        tEnv.registerDataSet("PersonTmp", dataStream, "name,age,city");

        Table table = tEnv.sqlQuery("SELECT * from " +
                "(SELECT * FROM Person WHERE age < 40) " +
                "UNION All " +
                "(SELECT * FROM PersonTmp WHERE age > 35) ");

        DataSet<Row> result = tEnv.toDataSet(table, Row.class);
        result.print();
    }


    @Test
    public void testDataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<Person> clicksData = PrepareData.getPersonData();
        DataStream<Person> dataStream = env.fromCollection(clicksData);

        tEnv.registerDataStream("Person", dataStream, "name,age,city");

        tEnv.registerDataStream("PersonTmp", dataStream, "name,age,city");

        Table table = tEnv.sqlQuery("SELECT * from " +
                "(SELECT * FROM Person WHERE age < 40) " +
                "UNION All " +
                "(SELECT * FROM PersonTmp WHERE age > 35) ");

        DataStream<Row> resultSet = tEnv.toAppendStream(table, Row.class);
        resultSet.print();
        env.execute();
    }


}
