package com.flink.sqlapi;

import com.flink.bean.ClickBean;
import com.flink.sql.PrepareData;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


public class SqlTemplate {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStream<ClickBean> streamSource = env.fromCollection(PrepareData.getClicksData());
        tableEnv.registerDataStream("Clicks", streamSource, "user,url,time");
        Table resultTable = tableEnv.sqlQuery("SELECT user,url FROM Clicks WHERE user = '张三'");
        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnv.toRetractStream(resultTable, Row.class);
        resultStream.print();
        env.execute();
    }
}
