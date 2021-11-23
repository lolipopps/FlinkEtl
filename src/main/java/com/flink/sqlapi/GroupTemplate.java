package com.flink.sqlapi;

import com.flink.sql.PrepareData;
import com.flink.bean.ClickBean;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;


import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;



public class GroupTemplate {



    @Test
    public void testDataSet() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        DataSet<ClickBean> streamSource = env.fromCollection(PrepareData.getClicksData());
        tableEnv.registerDataSet("Clicks", streamSource, "id,user,url,time");

        Table table = tableEnv.sqlQuery("SELECT user AS name, count(url) AS number FROM Clicks GROUP BY user HAVING count(1) > 3");

        DataSet<Row> result = tableEnv.toDataSet(table, Row.class);
        result.print();
    }


    @Test
    public void testDataStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStream<ClickBean> streamSource = env.fromCollection(PrepareData.getClicksData());

        tableEnv.registerDataStream("Clicks", streamSource, "id,user,url,time");

        Table table = tableEnv.sqlQuery("SELECT user AS name, count(url) AS number FROM Clicks GROUP BY user");
        DataStream<Tuple2<Boolean, Row>> toRetractStream = tableEnv.toRetractStream(table, Row.class);

        toRetractStream.print();
        env.execute();
    }

}
