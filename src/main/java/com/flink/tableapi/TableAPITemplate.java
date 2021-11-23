package com.flink.tableapi;

import com.flink.bean.ClickBean;
import com.flink.sql.PrepareData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.types.Row;



public class TableAPITemplate {


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);




        DataSet<ClickBean> dataStream = env.fromCollection(PrepareData.getClicksData());

        tableEnv.registerDataSet("Clicks", dataStream, "id,user,time,url");

        Table orders = tableEnv.scan("Clicks");

        Table result = orders
                .filter("user== '张三' || user=='李四'")
                .select("id,user,time");

        DataSet<Row> dataSet = tableEnv.toDataSet(result, Row.class);
        dataSet.print("result");
        env.execute();
    }
}
