package com.flink.sqlapi;

import com.flink.sql.PrepareData;
import com.flink.bean.Person;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import java.util.List;


public class UnionTemplate {



    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        List<Person> clicksData = PrepareData.getPersonData();
        DataSet<Person> dataStream = env.fromCollection(clicksData);

        tEnv.registerDataSet("Person", dataStream, "name,age,city");

        tEnv.registerDataSet("PersonTmp", dataStream, "name,age,city");

//        Table table = tEnv.sqlQuery("SELECT * FROM ( SELECT * FROM Person WHERE age < 23) UNION (SELECT * FROM PersonTmp WHERE age > 40)");
        Table table = tEnv.sqlQuery("SELECT * FROM ( SELECT name,age FROM Person WHERE age < 23) UNION (SELECT name,age FROM PersonTmp WHERE age > 40)");


        DataSet<Row> result = tEnv.toDataSet(table, Row.class);
        result.print();
    }

}
