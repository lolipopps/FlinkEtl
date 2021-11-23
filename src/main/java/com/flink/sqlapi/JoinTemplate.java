package com.flink.sqlapi;

import com.flink.sql.PrepareData;
import com.flink.bean.ClickBean;
import com.flink.bean.Person;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;


import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.Collections;
import java.util.List;


public class JoinTemplate {


    @Test
    public void testDataSet() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        List<ClickBean> clicksData = PrepareData.getClicksData();
        DataSet<ClickBean> clicksStream = env.fromCollection(clicksData);
        tEnv.registerDataSet("Clicks", clicksStream, "user,time,url");

        List<Person> personData = PrepareData.getPersonData();
        DataSet<Person> personStream = env.fromCollection(personData);
        tEnv.registerDataSet("Person", personStream, "name,age,city");

        tEnv.toDataSet(tEnv.sqlQuery("SELECT * FROM Clicks INNER JOIN Person ON name=user"), Row.class).print("INNER JOIN");

        tEnv.toDataSet(tEnv.sqlQuery("SELECT * FROM Clicks LEFT JOIN Person ON name=user"), Row.class).print("LEFT JOIN");
        tEnv.toDataSet(tEnv.sqlQuery("SELECT * FROM Clicks RIGHT JOIN Person ON name=user"), Row.class).print("RIGHT JOIN");
        tEnv.toDataSet(tEnv.sqlQuery("SELECT * FROM Clicks FULL JOIN Person ON name=user"), Row.class).print("FULL JOIN");

        env.execute();
    }


    @Test
    public void testDataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        List<ClickBean> clicksData = PrepareData.getClicksData();
        DataStream<ClickBean> clicksStream = env.fromCollection(clicksData);
        clicksStream = clicksStream.map((MapFunction<ClickBean, ClickBean>) value -> {
            Thread.sleep(2000);
            return value;
        });
        tEnv.registerDataStream("Clicks", clicksStream, "user,time,url");

        List<Person> personData = PrepareData.getPersonData();
        Collections.shuffle(personData);
        DataStream<Person> personStream = env.fromCollection(personData);
        personStream = personStream.map((MapFunction<Person, Person>) value -> {
            Thread.sleep(4000);
            return value;
        });
        tEnv.registerDataStream("Person", personStream, "name,age,city");


        tEnv.toAppendStream(tEnv.sqlQuery("SELECT * FROM Clicks INNER JOIN Person ON name=user"), Row.class).print("INNER JOIN");
        tEnv.toRetractStream(tEnv.sqlQuery("SELECT * FROM Clicks LEFT JOIN Person ON name=user"), Row.class).print("LEFT JOIN");
        tEnv.toRetractStream(tEnv.sqlQuery("SELECT * FROM Clicks RIGHT JOIN Person ON name=user"), Row.class).print("RIGHT JOIN");
        tEnv.toRetractStream(tEnv.sqlQuery("SELECT * FROM Clicks FULL JOIN Person ON name=user"), Row.class).print("FULL JOIN");

        env.execute();
    }


}
