package com.flink.sqlapi;

import com.flink.sql.PrepareData;
import com.flink.bean.ClickBean;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Timestamp;


public class TimeWindowJoinTemplate {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<ClickBean> dataStream = env.fromCollection(PrepareData.getClicksData());

        dataStream = dataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<ClickBean>() {

            @Override
            public long extractTimestamp(ClickBean element, long previousElementTimestamp) {
                Timestamp timestamp = element.getTime();
                return timestamp.getTime();
            }

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(System.currentTimeMillis());
            }
        });


        tableEnv.registerDataStream("Clicks", dataStream, "id,user,VisitTime.rowtime,url");

        String sqlQuery =
                "SELECT temp.name,temp.minId,id,temp.n,url,temp.betweenStart,temp.betweenTime FROM (" +
                        "SELECT user as name, " +
                        "count(url) as n ," +
                        "min(id) as minId," +
                        "TUMBLE_ROWTIME(VisitTime, INTERVAL '1' HOUR) as betweenTime," +
                        "TUMBLE_START(VisitTime, INTERVAL '1' HOUR) as betweenStart  " +
                        "FROM Clicks " +
                        "GROUP BY TUMBLE(VisitTime, INTERVAL '1' HOUR), user"
                        + ") temp LEFT JOIN Clicks ON temp.minId=Clicks.id " +
                        "AND Clicks.VisitTime <= temp.betweenTime AND Clicks.VisitTime >= temp.betweenTime - INTERVAL '1' HOUR";

        Table table = tableEnv.sqlQuery(sqlQuery);

        tableEnv.toAppendStream(table, Row.class).print();

        env.execute();

    }
}
