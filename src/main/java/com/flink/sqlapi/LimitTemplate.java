package com.flink.sqlapi;

import com.flink.sql.PrepareData;
import com.flink.bean.ClickBean;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.List;


public class LimitTemplate {


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        List<ClickBean> clicksData = PrepareData.getClicksData();
        Collections.shuffle(clicksData);
        System.out.println(clicksData);
        DataSet<ClickBean> dataStream = env.fromCollection(clicksData);

        tEnv.registerDataSet("Clicks", dataStream, "user,time,url");


        Table table = tEnv.sqlQuery("SELECT * FROM Clicks ORDER BY user ASC LIMIT 2");

        DataSet<Row> result = tEnv.toDataSet(table, Row.class);
        result.print();
    }


}
