package com.flink.sqlapi;

import com.flink.bean.PersonUnnestBean;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;


public class UnnestTemplate {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        List<PersonUnnestBean> personList = new ArrayList();
        personList.add(new PersonUnnestBean("张三", 38, new String[]{"上海", "浦东新区"}));
        personList.add(new PersonUnnestBean("李四", 45, new String[]{"深圳", "福田区"}));

        DataStream<PersonUnnestBean> personStream = env.fromCollection(personList);

        tEnv.registerDataStream("Person", personStream, "name,age,city");

        tEnv.toRetractStream(tEnv.sqlQuery("SELECT name,age,area FROM Person CROSS JOIN UNNEST(city) AS t (area)"), Row.class).print("UNNEST");

        env.execute();
    }
}
